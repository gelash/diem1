// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::parallel_executor::dependency_analyzer::TransactionParameters;
use crate::{
    data_cache::StateViewCache,
    diem_transaction_executor::{is_reconfiguration, PreprocessedTransaction},
    logging::AdapterLogSchema,
    parallel_executor::{
        data_cache::{VersionedDataCache, VersionedStateView},
        dependency_analyzer::DependencyAnalyzer,
        outcome_array::OutcomeArray,
        scheduler::Scheduler,
    },
    DiemVM,
};
use diem_state_view::StateView;
use diem_types::{access_path::AccessPath, transaction::TransactionOutput};
use move_core_types::vm_status::VMStatus;
use num_cpus;
use rand::Rng;
use rayon::{prelude::*, scope};
use std::time::Duration;
use std::time::Instant;
use std::{
    cmp::{max, min},
    collections::HashSet,
    convert::{TryFrom, TryInto},
    hint,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
};

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
    pub fn hack_infer_results(
        &self,
        prob_of_each_txn_to_drop: f64,
        percentage_of_each_txn_to_drop: f64,
        infer_result: Vec<(
            impl Iterator<Item = AccessPath>,
            impl Iterator<Item = AccessPath>,
        )>,
    ) -> Vec<(Vec<AccessPath>, HashSet<AccessPath>)> {
        let mut infer_results_after_hack = Vec::new();
        let mut rng = rand::thread_rng(); // randomness
        for (reads, writes) in infer_result {
            // randomly select some txn to drop their estimated write set
            if rng.gen_range(0.0..10.0) / 10.0 <= prob_of_each_txn_to_drop {
                // randomly drop the estimiated write set of the selected txn
                let writes_after_hack: HashSet<AccessPath> = writes
                    .filter(|_| rng.gen_range(0.0..10.0) / 10.0 > percentage_of_each_txn_to_drop)
                    .collect();
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
        base_view: &mut StateViewCache,
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
            DependencyAnalyzer::new_from_transactions(&signature_verified_block, base_view);
        let read_write_infer = match inferer {
            Err(_) => {
                return DiemVM::new(base_view)
                    .execute_block_impl(signature_verified_block, base_view)
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
                    return DiemVM::new(base_view)
                        .execute_block_impl(signature_verified_block, base_view)
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
        let infer_result = self.hack_infer_results(
            prob_of_each_txn_to_drop,
            percentage_of_each_txn_to_drop,
            infer_result_before_hack,
        );
        let infer_result_iters: Vec<_> = infer_result
            .iter()
            .map(|(x, y)| (x.iter(), y.iter()))
            .collect();

        let hack_infer_rwset_time = timer.elapsed();
        timer = Instant::now();

        // Analyse each user script for its write-set and create the placeholder structure
        // that allows for parallel execution.
        let path_version_tuples: Vec<(AccessPath, usize)> = infer_result_iters
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

        // Ensure we have at least 50 tx per thread and no higher rate of conflict than concurrency.
        let compute_threads = min(1 + (num_txns / 50), self.num_cpus - 1);
        let mut ones_mask = 1;
        // Set a bit to the left until compute_threads covered by the mask.
        while ones_mask < compute_threads {
            ones_mask = (ones_mask << 1) & 1;
        }
        // Threads mask is used to decide when each thread performs an expensive check whether
        // the execution is completed (all transactions can be committed from STM prospective).
        // Appending 4 1's to the right ensures the check happens once every 16 iterations.
        let threads_mask = (ones_mask << 4) & 15;

        let scheduler = Arc::new(Scheduler::new(num_txns, compute_threads));

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

        let thread_ids = AtomicUsize::new(0);
        scope(|s| {
            println!(
                "Launching {} threads to execute (Max conflict {}) ... total txns: {:?}, prob_of_each_txn_to_drop {}, percentage_of_each_txn_to_drop {}",
                compute_threads,
                max_dependency_level,
                scheduler.num_txn_to_execute(),
                prob_of_each_txn_to_drop,
                percentage_of_each_txn_to_drop,
            );
            for _ in 0..(compute_threads) {
                thread_ids.fetch_add(1, Ordering::SeqCst);
                s.spawn(|_| {
                    let thread_id = thread_ids.load(Ordering::SeqCst)-1;
                    let scheduler = Arc::clone(&scheduler);
                    // Make a new VM per thread -- with its own module cache
                    let thread_vm = DiemVM::new(base_view);

                    let mut local_execution_time = Duration::new(0, 0);
                    let mut local_checking_time = Duration::new(0, 0);
                    let mut local_apply_write_time = Duration::new(0, 0);
                    let mut local_other_time = Duration::new(0, 0);
                    let mut local_validation_time = Duration::new(0, 0);
                    let mut local_validation_read_time = Duration::new(0, 0);
                    let mut local_validation_write_time = Duration::new(0, 0);
                    let mut local_timer = Instant::now();

                    let mut i: usize = 0;
                    loop {
                        if i >> 4 == thread_id {
                            // Every once in a while check if all txns are committed.
                            scheduler.check_done();
                        }
                        i = (i + 1) & threads_mask;
                        if scheduler.done() {
                            // The work is done, break from the loop.
                            break;
                        }

                        let mut version_to_execute = None;
                        // First check if any txn can be validated
                        if let Some((version_to_validate, status_to_validate)) =
                            scheduler.next_txn_to_validate(thread_id)
                        {
                            // There is a txn to be validated

                            let state_view = VersionedStateView::new(
                                version_to_validate,
                                base_view,
                                &versioned_data_cache,
                            );

                            let read_timer = Instant::now();
                            let valid = status_to_validate.read_set().iter().all(|r| {
                                r.validate(state_view.read_version_and_retry_num(r.path()))
                            });
                            local_validation_read_time += read_timer.elapsed();

                            if valid {
                                scheduler.finish_validation(thread_id);

                                local_validation_time += local_timer.elapsed();
                                local_timer = Instant::now();

                                // Validation successfully completed, continue to the work loop.
                                continue;
                            } else {
                                if !scheduler.abort(version_to_validate, &status_to_validate) {
                                    local_validation_time += local_timer.elapsed();
                                    local_timer = Instant::now();

                                    // Someone already aborted, continue to the work loop.
                                    continue;
                                }

                                // Set dirty in both static and dynamic mvhashmaps.
                                let write_timer = Instant::now();
                                versioned_data_cache.mark_dirty(
                                    version_to_validate,
                                    status_to_validate.retry_num(),
                                    &infer_result[version_to_validate].1,
                                    status_to_validate.write_set(),
                                );
                                local_validation_write_time += write_timer.elapsed();

                                // Thread will immediately re-execute this txn (bypass global scheduling).
                                // Rationale is to avoid overhead, and also aborted txn is high priority
                                // to re-execute asap to enable successfully executing/validating subsequent txns.
                                version_to_execute = Some(version_to_validate);

                                revalidation_counter.fetch_add(1, Ordering::Relaxed);
                            }
                            local_validation_time += local_timer.elapsed();
                            local_timer = Instant::now();
                        }

                        // If there is no txn to be committed or validated, get the next txn to execute
                        if version_to_execute.is_none() {
                            version_to_execute = scheduler.next_txn_to_execute();

                            if version_to_execute.is_none() {
                                // This causes a PAUSE on an x64 arch, and takes 140 cycles. Allows other
                                // core to take resources and better HT.
                                hint::spin_loop();

                                local_other_time += local_timer.elapsed();
                                local_timer = Instant::now();

                                // Nothing to execute, continue to the work loop.
                                continue;
                            }
                        }

                        let txn_to_execute = version_to_execute.unwrap();

                        let txn = &signature_verified_block[txn_to_execute];

                        let state_view = VersionedStateView::new(
                            txn_to_execute,
                            base_view,
                            &versioned_data_cache,
                        );

                        // Check read dependencies - if unresolved read dependency found, don't execute txn,
                        // add the dependency (just the first one found) to the scheduler before attempting
                        // to re-execute and continue to the work loop.
                        if infer_result[txn_to_execute].0.iter().any(|k| {
                            match state_view.read_blocking_dependency(&k, txn_to_execute) {
                                None => false,
                                Some(dep_id) => scheduler.add_dependency(txn_to_execute, dep_id),
                            }
                        }) {
                            delay_execution_counter.fetch_add(1, Ordering::Relaxed);
                            local_checking_time += local_timer.elapsed();

                            // // This causes a PAUSE on an x64 arch, and takes 140 cycles. Allows other
                            // // core to take resources and better HT.
                            hint::spin_loop();

                            local_timer = Instant::now();
                            continue;
                        }

                        local_checking_time += local_timer.elapsed();
                        local_timer = Instant::now();

                        // Execute the transaction
                        let log_context = AdapterLogSchema::new(state_view.id(), txn_to_execute);
                        let res =
                            thread_vm.execute_single_transaction(txn, &state_view, &log_context);

                        local_execution_time += local_timer.elapsed();
                        local_timer = Instant::now();

                        match res {
                            Ok((vm_status, output, _sender)) => {
                                let retry_num = scheduler.retry_num(txn_to_execute);
                                versioned_data_cache.apply_output(
                                    &output,
                                    txn_to_execute,
                                    retry_num,
                                    &infer_result[txn_to_execute].1,
                                );

                                if is_reconfiguration(&output) {
                                    // This transacton is correct, but all subsequent transactions must be rejected
                                    // (with retry status) since it forced a reconfiguration.
                                    println!("Txn {} is_reconfiguration", txn_to_execute);
                                    scheduler.set_stop_version(txn_to_execute + 1);
                                }

                                scheduler.finish_execution(
                                    txn_to_execute,
                                    retry_num,
                                    state_view.drain_reads(),
                                    (vm_status, output),
                                );
                            }
                            Err(_e) => {
                                panic!("TODO STOP VM & RETURN ERROR");
                            }
                        }
                        local_apply_write_time += local_timer.elapsed();
                        local_timer = Instant::now();
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
                    loop_vec.push(
                        local_execution_time.as_millis()
                            + local_checking_time.as_millis()
                            + local_other_time.as_millis(),
                    );
                });
            }
        });

        let execution_loop_time = timer.elapsed();
        timer = Instant::now();
        let valid_results_length = scheduler.num_txn_to_execute();

        // Set outputs in parallel
        let commit_index = AtomicUsize::new(0);
        scope(|s| {
            // How many threads to use?
            let compute_cpus = min(1 + (num_txns / 50), self.num_cpus - 1); // Ensure we have at least 50 tx per thread.
            let compute_cpus = min(num_txns / max(1, max_dependency_level), compute_cpus); // Ensure we do not higher rate of conflict than concurrency.
            for _ in 0..(compute_cpus) {
                s.spawn(|_| {
                    loop {
                        let next_to_commit = commit_index.load(Ordering::Relaxed);
                        if next_to_commit >= valid_results_length {
                            break;
                        }
                        match commit_index.compare_exchange(
                            next_to_commit,
                            next_to_commit + 1,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => {
                                let (vm_status, output) = scheduler.output(next_to_commit);
                                // Updates the outcome array
                                let success = !output.status().is_discarded();
                                outcomes.set_result(next_to_commit, (vm_status, output), success);
                            }
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
        println!(
            "Number of execution delays: {}",
            delay_execution_counter.load(Ordering::Relaxed)
        );
        println!(
            "Number of re-validation: {}",
            revalidation_counter.load(Ordering::Relaxed)
        );
        // println!(
        // "Number of re-executions: {}",
        // scheduler.sum_retry_num() - valid_results_length
        // );

        let ((s_max, s_avg), (d_max, d_avg)) = versioned_data_cache.get_depth();
        println!("Static mvhashmap: max depth {}, avg depth {}", s_max, s_avg);
        println!(
            "Dynamic mvhashmap: max depth {}, avg depth {}\n",
            d_max, d_avg
        );

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

        // scheduler.print_info();

        all_results
    }
}
