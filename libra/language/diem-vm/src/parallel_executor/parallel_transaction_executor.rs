// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::parallel_executor::dependency_analyzer::TransactionParameters;
use crate::parallel_executor::scheduler::Scheduler;
use crate::{
    data_cache::StateViewCache,
    diem_transaction_executor::{
        is_reconfiguration, PreprocessedTransaction,
    },
    logging::AdapterLogSchema,
    parallel_executor::{
        data_cache::{VersionedDataCache, VersionedStateView},
        dependency_analyzer::DependencyAnalyzer,
        outcome_array::OutcomeArray,
    },
    DiemVM,
};
use diem_state_view::StateView;
use diem_types::{
    access_path::AccessPath,
    transaction::TransactionOutput,
};
use move_core_types::vm_status::VMStatus;
use num_cpus;
use rayon::{prelude::*, scope};
use std::{
    thread,
    cmp::{max, min},
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    convert::{TryFrom,TryInto},
};
use rand::Rng;
use std::time::Duration;
use std::time::Instant;

pub struct ParallelTransactionExecutor {
    num_cpus: usize,
    txn_per_thread: u64,
}

impl ParallelTransactionExecutor {
    pub fn new() -> Self {
        Self {
            num_cpus: num_cpus::get(),
            txn_per_thread: 50,
        }
    }

    // Randomly drop some estimiated writes in the analyzed writeset, in order to test STM
    pub fn hack_infer_results(&self, prob_of_each_txn_to_drop: f64, percentage_of_each_txn_to_drop: f64, infer_result: Vec<(impl Iterator<Item = AccessPath>, impl Iterator<Item = AccessPath>)>)
     -> Vec<(Vec<AccessPath>, Vec<AccessPath>)> {
        let mut infer_results_after_hack = Vec::new();
        let mut rng = rand::thread_rng(); // randomness
        for (reads, writes) in infer_result {
            // randomly select some txn to drop their estimated write set
            if rng.gen_range(0.0..10.0)/10.0 <= prob_of_each_txn_to_drop {
                // randomly drop the estimiated write set of the selected txn
                let writes_after_hack: Vec<AccessPath> = writes.filter(|_| rng.gen_range(0.0..10.0)/10.0 > percentage_of_each_txn_to_drop).collect();
                infer_results_after_hack.push((reads.collect(), writes_after_hack));
            } else {
                infer_results_after_hack.push((reads.collect(), writes.collect()));
            }
        }
        return infer_results_after_hack;
    }

    pub(crate) fn execute_transactions_parallel(
        &self,
        signature_verified_block: Vec<PreprocessedTransaction>,
        data_cache: &mut StateViewCache,
    ) -> Result<Vec<(VMStatus, TransactionOutput)>, VMStatus> {
        let timer_start = Instant::now();
        let mut timer = Instant::now();

        let num_txns = signature_verified_block.len();
        let chunks = max(1, num_txns / self.num_cpus);

        // Update the dependency analysis structure. We only do this for blocks that
        // purely consist of UserTransactions (Extending this is a TODO). If non-user
        // transactions are detected this returns and err, and we revert to sequential
        // block processing.

        let inferer =
            DependencyAnalyzer::new_from_transactions(&signature_verified_block, data_cache);
        let read_write_infer = match inferer {
            Err(_) => {
                return DiemVM::new(data_cache)
                    .execute_block_impl(signature_verified_block, data_cache)
            }
            Ok(val) => val,
        };

        let args: Vec<_> = signature_verified_block
            .par_iter()
            .with_min_len(chunks)
            .map(TransactionParameters::new_from)
            .collect();

        let infer_result_before_hack: Vec<_> = {
            match signature_verified_block
                .par_iter()
                .zip(args.par_iter())
                .with_min_len(chunks)
                .map(|(txn, args)| read_write_infer.get_inferred_read_write_set(txn, args))
                .collect::<Result<Vec<_>, VMStatus>>()
            {
                Ok(res) => res,
                Err(_) => {
                    return DiemVM::new(data_cache)
                        .execute_block_impl(signature_verified_block, data_cache)
                }
            }
        };

        let infer_rwset_time = timer.elapsed();
        timer = Instant::now();

        // let mut rng = rand::thread_rng(); // randomness
        // let prob_of_each_txn_to_drop = rng.gen_range(0.0..1.0);
        // let percentage_of_each_txn_to_drop = rng.gen_range(0.0..1.0);
        let prob_of_each_txn_to_drop = 1.0;
        let percentage_of_each_txn_to_drop = 1.0;
        // Randomly drop some estimated writeset of some transaction
        let tmp = self.hack_infer_results(prob_of_each_txn_to_drop, percentage_of_each_txn_to_drop, infer_result_before_hack);
        let infer_result: Vec<_> = tmp.iter().map(|(x, y)| (x.iter(), y.iter())).collect();

        let hack_infer_rwset_time = timer.elapsed();
        timer = Instant::now();

        // Analyse each user script for its write-set and create the placeholder structure
        // that allows for parallel execution.
        let path_version_tuples: Vec<(AccessPath, usize)> = infer_result
            .par_iter()
            .enumerate()
            .with_min_len(chunks)
            .fold(
                || Vec::new(),
                |mut acc, (idx, (_, txn_writes))| {
                    acc.extend(txn_writes.clone().map(|ap| (ap.clone(), idx)));
                    acc
                },
            )
            .reduce(
                || Vec::new(),
                |mut lhs, mut rhs| {
                    lhs.append(&mut rhs);
                    lhs
                },
            );

        let ((max_dependency_level, versioned_data_cache), outcomes) = rayon::join(
            || VersionedDataCache::new(path_version_tuples),
            || OutcomeArray::new(num_txns),
        );

        let scheduler = Arc::new(Scheduler::new(num_txns));
        let delay_execution_counter = AtomicUsize::new(0);
        let revalidation_counter = AtomicUsize::new(0);

        let mvhashmap_construction_time = timer.elapsed();
        timer = Instant::now();

        let execution_time = Arc::new(Mutex::new(Duration::new(0, 0)));
        let checking_time = Arc::new(Mutex::new(Duration::new(0, 0)));
        let validation_time = Arc::new(Mutex::new(Duration::new(0, 0)));
        let apply_write_time = Arc::new(Mutex::new(Duration::new(0, 0)));
        let other_time = Arc::new(Mutex::new(Duration::new(0, 0)));
        let validation_read_time = Arc::new(Mutex::new(Duration::new(0, 0)));
        let validation_write_time = Arc::new(Mutex::new(Duration::new(0, 0)));
        let loop_time_vec = Arc::new(Mutex::new(Vec::new()));
        let execution_time_vec = Arc::new(Mutex::new(Vec::new()));

        scope(|s| {
            // How many threads to use?
            let compute_cpus = min(1 + (num_txns / 50), self.num_cpus - 1); // Ensure we have at least 50 tx per thread.
            // let compute_cpus = min(num_txns / max(1, max_dependency_level), compute_cpus); // Ensure we do not higher rate of conflict than concurrency.

            println!(
                "Launching {} threads to execute (Max conflict {}) ... total txns: {:?}, prob_of_each_txn_to_drop {}, percentage_of_each_txn_to_drop {}",
                compute_cpus,
                max_dependency_level,
                scheduler.get_txn_num(),
                prob_of_each_txn_to_drop,
                percentage_of_each_txn_to_drop,
            );
            for _ in 0..(compute_cpus) {
                s.spawn(|_| {
                    let scheduler = Arc::clone(&scheduler);
                    // Make a new VM per thread -- with its own module cache
                    let thread_vm = DiemVM::new(data_cache);

                    let mut local_execution_time = Duration::new(0, 0);
                    let mut local_checking_time = Duration::new(0, 0);
                    let mut local_validation_time = Duration::new(0, 0);
                    let mut local_apply_write_time = Duration::new(0, 0);
                    let mut local_other_time = Duration::new(0, 0);
                    let mut local_validation_read_time = Duration::new(0, 0);
                    let mut local_validation_write_time = Duration::new(0, 0);
                    let mut local_timer = Instant::now();

                    loop {
                        // All txns are committed, break from the loop
                        if scheduler.all_committed() {
                            break;
                        }
                        // First check if any txn can be validated
                        let result = scheduler.next_txn_to_validate();
                        let mut txn_to_execute = None;
                        let mut back_num = scheduler.get_val_index_back_num();
                        if result.is_some() {
                            // There is some txn to be validated
                            let txn_to_validate = result.unwrap().0;
                            back_num = result.unwrap().1;
                            // println!("Thread {:?} processing txn {} check validation...", thread::current().id(), txn_to_validate);

                            if txn_to_validate >= scheduler.get_txn_num() {
                                // scheduler.print_info();
                                // This causes a PAUSE on an x64 arch, and takes 140 cycles. Allows other
                                // core to take resources and better HT.
                                ::std::sync::atomic::spin_loop_hint();
                                continue;
                            }

                            let (reads, writes) = &infer_result[txn_to_validate];

                            let tmp_timer = Instant::now();
                            let versioned_state_view =
                                VersionedStateView::new(txn_to_validate, data_cache, &versioned_data_cache);
                            let validation_reads_version_vec = versioned_state_view.get_reads_version_vec(reads.clone().map(|x| x.clone()));
                            local_validation_read_time += tmp_timer.elapsed();

                            if scheduler.validation(txn_to_validate, validation_reads_version_vec) {
                                // println!("Thread {:?} processing txn {} validation succeed...", thread::current().id(), txn_to_validate);
                                // If the validation succeeds, update the status_vec as validated
                                if scheduler.set_validated(txn_to_validate, back_num) {
                                    // Commit the prefix of txns that are all validated
                                    scheduler.commit();
                                }

                                local_validation_time += local_timer.elapsed();
                                local_timer = Instant::now();

                                continue;
                            } else {
                                // println!("Thread {:?} processing txn {} validation failed...", thread::current().id(), txn_to_validate);

                                // abort will set the txn status to be FLAG_NOT_VALIDATED and decrease val_index
                                // abort return (true, new_back_num) if successful, otherwise (false, back_num)
                                let result = scheduler.abort(txn_to_validate, back_num);
                                if !result.0 {
                                    continue;
                                }
                                back_num = result.1;

                                // Set dirty to both static and dynamic mvhashmap
                                let last_execution_result = scheduler.get_last_output(txn_to_validate);
                                if last_execution_result.is_none() {
                                    println!("ERROR when get last execution result!");
                                    continue;
                                }

                                let tmp_timer = Instant::now();

                                let (_, last_execution_output) = last_execution_result.unwrap();
                                // Get non-estimated writes set
                                let estimated_writes: HashSet<AccessPath> = writes.clone().map(|x| x.clone()).collect();
                                let mut non_estimated_writes: HashSet<AccessPath> = HashSet::new();
                                for (k, _) in last_execution_output.write_set() {
                                    if !estimated_writes.contains(k) {
                                        non_estimated_writes.insert(k.clone());
                                    }
                                }
                                let retry_num = scheduler.get_retry_num(txn_to_validate);
                                versioned_data_cache.set_dirty_to_static(txn_to_validate, retry_num, estimated_writes);
                                versioned_data_cache.set_dirty_to_dynamic(txn_to_validate, retry_num, non_estimated_writes);

                                local_validation_write_time += tmp_timer.elapsed();

                                // Immediately re-execute this txn
                                txn_to_execute = Some(txn_to_validate);

                                revalidation_counter.fetch_add(1, Ordering::Relaxed);
                            }
                            local_validation_time += local_timer.elapsed();
                            local_timer = Instant::now();
                        }
                        // println!("Thread {:?} passes validation...", thread::current().id());

                        // If there is no txn to be committed or validated, get the next txn to execute
                        if txn_to_execute.is_none() {
                            txn_to_execute = scheduler.next_txn_to_execute();
                        }
                        if txn_to_execute.is_none() {
                            // scheduler.print_info();
                            // This causes a PAUSE on an x64 arch, and takes 140 cycles. Allows other
                            // core to take resources and better HT.
                            ::std::sync::atomic::spin_loop_hint();
                            continue;
                        }
                        let txn_to_execute = txn_to_execute.unwrap();
                        // println!("Thread {:?} processing txn {} check execution...", thread::current().id(), txn_to_execute);

                        let txn = &signature_verified_block[txn_to_execute];
                        let (reads, writes) = &infer_result[txn_to_execute];

                        let versioned_state_view =
                            VersionedStateView::new(txn_to_execute, data_cache, &versioned_data_cache);

                        local_other_time += local_timer.elapsed();
                        local_timer = Instant::now();

                        // If the txn has unresolved dependency, adds the txn to deps_mapping of its dependency (only the first one) and continue
                        if reads.clone().any(|k| {
                            versioned_state_view
                                .will_read_from_both_block_return_version(&k)
                                .and_then(|dep_id| scheduler.update_read_deps(txn_to_execute, dep_id))
                                .unwrap_or(false)
                        }) {
                            delay_execution_counter.fetch_add(1, Ordering::Relaxed);
                            local_checking_time += local_timer.elapsed();
                            local_timer = Instant::now();
                            // println!("Thread {:?} processing txn {} needs re-execution...", thread::current().id(), txn_to_execute);

                            // // This causes a PAUSE on an x64 arch, and takes 140 cycles. Allows other
                            // // core to take resources and better HT.
                            // ::std::sync::atomic::spin_loop_hint();
                            continue;
                        }

                        // println!("Thread {:?} processing txn {} check reads done...", thread::current().id(), txn_to_execute);
                        // scheduler.print_info();

                        local_checking_time += local_timer.elapsed();
                        local_timer = Instant::now();

                        let tmp_timer = Instant::now();
                        // Execute the reads TODO: this should be part of the txn execution below
                        let last_reads_version_vec = versioned_state_view.get_reads_version_vec(reads.clone().map(|x| x.clone()));
                        scheduler.update_last_reads(txn_to_execute, last_reads_version_vec);
                        local_validation_read_time += tmp_timer.elapsed();
                        local_validation_time += local_timer.elapsed();
                        local_timer = Instant::now();

                        // println!("Thread {:?} processing txn {} get reads done...", thread::current().id(), txn_to_execute);

                        // Execute the transaction
                        let log_context = AdapterLogSchema::new(versioned_state_view.id(), txn_to_execute);
                        let res = thread_vm.execute_single_transaction(
                            txn,
                            &versioned_state_view,
                            &log_context,
                        );
                        // println!("Thread {:?} processing txn {} execute_single_transaction done...", thread::current().id(), txn_to_execute);

                        local_execution_time += local_timer.elapsed();
                        local_timer = Instant::now();

                        match res {
                            Ok((vm_status, output, _sender)) => {
                                let retry_num = scheduler.increment_retry_num(txn_to_execute);
                                // println!("Thread {:?} processing txn {} start apply_output...", thread::current().id(), txn_to_execute);
                                if versioned_data_cache
                                    .apply_output(&output, txn_to_execute, retry_num, writes.clone().map(|x| x.clone()))
                                    .is_err()
                                {
                                    // An error occured when estimating the write-set of this transaction.
                                    // We therefore cut the execution of the block short here. We set
                                    // decrese the transaction index at which we stop, by seeting it
                                    // to be this one or lower.
                                    println!("Adjust boundary {}", txn_to_execute);
                                    scheduler.set_stop_version(txn_to_execute);
                                    continue;
                                }
                                // println!("Thread {:?} processing txn {} end apply_output...", thread::current().id(), txn_to_execute);

                                scheduler.update_last_output(txn_to_execute, (vm_status, output.clone()));
                                scheduler.update_dep_after_execution(txn_to_execute);
                                scheduler.set_executed(txn_to_execute, back_num);

                                if is_reconfiguration(&output) {
                                    // This transacton is correct, but all subsequent transactions
                                    // must be rejected (with retry status) since it forced a
                                    // reconfiguration.
                                    println!("Txn {} is_reconfiguration", txn_to_execute);
                                    scheduler.set_stop_version(txn_to_execute + 1);
                                    continue;
                                }
                            }
                            Err(_e) => {
                                panic!("TODO STOP VM & RETURN ERROR");
                            }
                        }
                        local_apply_write_time += local_timer.elapsed();
                        local_timer = Instant::now();
                        // println!("Thread {:?} processing txn {} back of loop...", thread::current().id(), txn_to_execute);
                    }
                    let mut execution = execution_time.lock().unwrap();
                    *execution = max(local_execution_time, *execution);
                    let mut checking = checking_time.lock().unwrap();
                    *checking = max(local_checking_time, *checking);
                    let mut validation = validation_time.lock().unwrap();
                    *validation = max(local_validation_time, *validation);
                    let mut apply_write = apply_write_time.lock().unwrap();
                    *apply_write = max(local_apply_write_time, *apply_write);
                    let mut other = other_time.lock().unwrap();
                    *other = max(local_other_time, *other);
                    let mut validation_read = validation_read_time.lock().unwrap();
                    *validation_read = max(local_validation_read_time, *validation_read);
                    let mut validation_write = validation_write_time.lock().unwrap();
                    *validation_write = max(local_validation_write_time, *validation_write);
                    let mut execution_vec = execution_time_vec.lock().unwrap();
                    execution_vec.push(local_execution_time.as_millis());
                    let mut loop_vec = loop_time_vec.lock().unwrap();
                    loop_vec.push(local_execution_time.as_millis()+local_checking_time.as_millis()+local_other_time.as_millis());
                });
            }
        });

        let execution_loop_time = timer.elapsed();
        timer = Instant::now();
        let valid_results_length = scheduler.get_txn_num();

        // Set outputs in parallel
        let commit_index = AtomicUsize::new(0);
        scope(|s| {
            // How many threads to use?
            let compute_cpus = min(1 + (num_txns / 50), self.num_cpus - 1); // Ensure we have at least 50 tx per thread.
            let compute_cpus = min(num_txns / max(1, max_dependency_level), compute_cpus); // Ensure we do not higher rate of conflict than concurrency.
            for _ in 0..(compute_cpus) {
                s.spawn(|_| {
                    loop {
                        let next_to_commit = commit_index.load(Ordering::SeqCst);
                        if next_to_commit >= valid_results_length {
                            break;
                        }
                        match commit_index.compare_exchange(next_to_commit, next_to_commit+1, Ordering::SeqCst, Ordering::SeqCst) {
                            Ok(_) => {
                                let (vm_status, output) = scheduler.get_last_output(next_to_commit).unwrap();
                                // Updates the outcome array
                                let success = !output.status().is_discarded();
                                outcomes.set_result(next_to_commit, (vm_status, output), success);
                            },
                            Err(_) => continue,
                        }
                    }
                });
            }
        });

        let set_output_time = timer.elapsed();
        timer = Instant::now();

        // Splits the head of the vec of results that are valid
        // println!("Valid length: {}", valid_results_length);
        println!("Number of execution delays: {}", delay_execution_counter.load(Ordering::Relaxed));
        println!("Number of re-validation: {}", revalidation_counter.load(Ordering::Relaxed));
        println!("Number of re-executions: {}", scheduler.sum_retry_num() - valid_results_length);

        let ((s_max, s_avg), (d_max, d_avg)) = versioned_data_cache.get_depth();
        println!("Static mvhashmap: max depth {}, avg depth {}", s_max, s_avg);
        println!("Dynamic mvhashmap: max depth {}, avg depth {}\n", d_max, d_avg);

        let all_results = outcomes.get_all_results(valid_results_length);

        drop(infer_result);

        // Dropping large structures is expensive -- do this is a separate thread.
        ::std::thread::spawn(move || {
            drop(signature_verified_block); // Explicit drops to measure their cost.
            drop(versioned_data_cache);
        });

        assert!(all_results.as_ref().unwrap().len() == valid_results_length);

        let remaining_time = timer.elapsed();
        let total_time = timer_start.elapsed();
        println!("=====PERF=====\n infer_rwset_time {:?}\n hack_infer_rwset_time {:?}\n mvhashmap_construction_time {:?}\n execution_loop_time {:?}\n set_output_time {:?}\n remaining_time {:?}\n\n total_time {:?}\n total_time without rw_analysis {:?}\n", infer_rwset_time, hack_infer_rwset_time, mvhashmap_construction_time, execution_loop_time, set_output_time, remaining_time, total_time, total_time-infer_rwset_time-hack_infer_rwset_time);
        println!("=====INSIDE THE LOOP (max among all threads)=====\n execution_time {:?}\n apply_write_time {:?}\n checking_time {:?}\n validation_time {:?}\n validation_read_time {:?}\n validation_write_time {:?}\n other_time {:?}\n execution_time_vec {:?}\n loop_time_vec {:?}\n", execution_time, apply_write_time, checking_time, validation_time, validation_read_time, validation_write_time, other_time, execution_time_vec, loop_time_vec);

        scheduler.print_info();

        all_results
    }
}
