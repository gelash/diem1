// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    errors::*,
    outcome_array::OutcomeArray,
    scheduler::{ReadDescriptor, Scheduler},
    task::{ExecutionStatus, ExecutorTask, ReadWriteSetInferencer, Transaction, TransactionOutput},
};
use anyhow::{bail, Result as AResult};
use mvhashmap::{MVHashMap, Version};
use num_cpus;
use rayon::{prelude::*, scope};
use std::{
    cmp::{max, min},
    collections::HashSet,
    hash::Hash,
    hint,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

pub struct MVHashMapView<'a, K, V, T, E> {
    map: &'a MVHashMap<K, V>,
    version: Version,
    scheduler: &'a Scheduler<K, T, E>,
    read_dependency: AtomicBool,
    reads: Arc<Mutex<Vec<ReadDescriptor<K>>>>,
}

const NOTHING_ITER_THRESHOLD: usize = 10000;

impl<
        'a,
        K: PartialOrd + Send + Clone + Hash + Eq,
        V: Clone + Send + Sync,
        T: TransactionOutput,
        E: Send + Clone,
    > MVHashMapView<'a, K, V, T, E>
{
    // Drains the reads.
    pub fn drain_reads(&self) -> Vec<ReadDescriptor<K>> {
        let mut reads = self.reads.lock().unwrap();
        std::mem::take(&mut reads)
    }

    pub fn read_map(
        &self,
        key: &K,
    ) -> std::result::Result<(Option<V>, Version, usize), Option<(Version, usize)>> {
        self.map.read(key, self.version)
    }

    pub fn read(&self, key: &K) -> AResult<Option<V>> {
        loop {
            match self.map.read(key, self.version) {
                Ok((v, version, exec_id)) => {
                    self.reads
                        .lock()
                        .unwrap()
                        .push(ReadDescriptor::new(key.clone(), Some((version, exec_id))));
                    return Ok(v);
                }
                Err(None) => {
                    self.reads
                        .lock()
                        .unwrap()
                        .push(ReadDescriptor::new(key.clone(), None));
                    return Ok(None);
                }
                Err(Some((dep_idx, _exec_id))) => {
                    // Don't start execution transaction `self.version` until `dep_idx` is computed.
                    if self.scheduler.add_dependency(self.version, dep_idx) {
                        // dep_idx is already executed, push `self.version` to ready queue.
                        // self.scheduler.add_transaction(self.version);
                        self.read_dependency.store(true, Ordering::Relaxed);
                        bail!("Read dependency is not computed, retry later")
                    }
                }
            };
        }
    }

    // Apply the writes to both static and dynamic mvhashmap
    pub fn write(
        &self,
        k: &K,
        version: Version,
        exec_id: usize,
        data: Option<V>,
        original_estimates: &HashSet<K>,
        prev_writes: &mut HashSet<K>,
        writes_outside: &mut bool,
    ) -> Result<(), Error<E>> {
        if !prev_writes.remove(k) {
            *writes_outside = true;
        }

        // Write estimated writes to static mvhashmap, and write non-estimated ones to dynamic mvhashmap
        if original_estimates.contains(k) {
            self.map.write_to_static(k, version, exec_id, data).unwrap();
        } else {
            self.map
                .write_to_dynamic(k, version, exec_id, data)
                .unwrap();
        }
        Ok(())
    }

    pub fn mark_dirty(
        &self,
        version: usize,
        exec_id: usize,
        original_estimates: &HashSet<K>,
        actual_writes: &Vec<(K, V)>,
    ) {
        for (k, _) in actual_writes {
            if !original_estimates.contains(k) {
                self.map.set_dirty_to_dynamic(k, version, exec_id);
            } else {
                self.map.set_dirty_to_static(k, version, exec_id).unwrap();
            }
        }
    }

    pub fn skip(&self, version: usize, skipped_writes: HashSet<K>, original_estimates: HashSet<K>) {
        for k in skipped_writes.iter() {
            if !original_estimates.contains(k) {
                self.map.skip_dynamic(k, version);
            } else {
                self.map.skip_static(k, version).unwrap();
            }
        }
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn read_dependency(&self) -> bool {
        self.read_dependency.load(Ordering::Relaxed)
    }
}

pub struct ParallelTransactionExecutor<T: Transaction, E: ExecutorTask, I: ReadWriteSetInferencer> {
    num_cpus: usize,
    inferencer: I,
    phantom: PhantomData<(T, E, I)>,
}

impl<T, E, I> ParallelTransactionExecutor<T, E, I>
where
    T: Transaction,
    E: ExecutorTask<T = T>,
    I: ReadWriteSetInferencer<T = T>,
{
    pub fn new(inferencer: I) -> Self {
        Self {
            num_cpus: num_cpus::get(),
            inferencer,
            phantom: PhantomData,
        }
    }

    pub fn execute_transactions_parallel(
        &self,
        task_initial_arguments: E::Argument,
        signature_verified_block: Vec<T>,
    ) -> Result<Vec<E::Output>, E::Error> {
        if signature_verified_block.is_empty() {
            return Ok(vec![]);
        }

        let timer_start = Instant::now();
        let mut timer = Instant::now();

        let num_txns = signature_verified_block.len();
        let chunks_size = max(1, num_txns / self.num_cpus);

        // let timer1 = Instant::now();
        // Get the read and write dependency for each transaction.
        let infer_result = self.inferencer.result(&signature_verified_block);

        // Use write analysis result to construct placeholders.
        let path_version_tuples: Vec<(T::Key, usize)> = infer_result
            .par_iter()
            .enumerate()
            .with_min_len(chunks_size)
            .fold(Vec::new, |mut acc, (idx, accesses)| {
                acc.extend(
                    accesses
                        .keys_written
                        .clone()
                        .into_iter()
                        .map(|ap| (ap, idx)),
                );
                acc
            })
            .flatten()
            .collect();

        let (versioned_data_cache, max_dependency_level) =
            MVHashMap::new_from_parallel(path_version_tuples);

        // if max_dependency_level == 0 {
        //     return Err(Error::InferencerError);
        // }

        let outcomes = OutcomeArray::new(num_txns);

        let scheduler = Scheduler::new(num_txns);

        let delay_execution_counter = AtomicUsize::new(0);
        let abort_counter = AtomicUsize::new(0);
        let validation_counter = AtomicUsize::new(0);

        let mvhashmap_construction_time = timer.elapsed();

        let dep_execution_time = Arc::new(Mutex::new(Duration::new(0, 0)));
        let execution_time = Arc::new(Mutex::new(Duration::new(0, 0)));
        let checking_time = Arc::new(Mutex::new(Duration::new(0, 0)));
        let apply_write_time = Arc::new(Mutex::new(Duration::new(0, 0)));

        let other_time = Arc::new(Mutex::new(Duration::new(0, 0)));

        let validation_time = Arc::new(Mutex::new(Duration::new(0, 0)));
        //let validation_read_time = Arc::new(Mutex::new(Duration::new(0, 0)));
        //let validation_write_time = Arc::new(Mutex::new(Duration::new(0, 0)));

        let execution_time_vec = Arc::new(Mutex::new(Vec::new()));
        let validation_time_vec = Arc::new(Mutex::new(Vec::new()));
        let other_time_vec = Arc::new(Mutex::new(Vec::new()));
        let loop_time_vec = Arc::new(Mutex::new(Vec::new()));

        timer = Instant::now();

        let id_gen = AtomicUsize::new(0);
        let compute_cpus = self.num_cpus;
        let validator_workers = AtomicUsize::new(compute_cpus / 4);
        let max_validator_workers = compute_cpus / 2;

        scope(|s| {
            println!(
                "Launching {} threads to execute (Max conflict {}) ... total txns: {:?}",
                compute_cpus,
                max_dependency_level,
                scheduler.num_txn_to_execute(),
            );

            for _ in 0..(compute_cpus) {
                s.spawn(|_| {
                    // Make a new executor per thread.
                    let id: usize = id_gen.fetch_add(1, Ordering::Relaxed);

                    let task = E::init(task_initial_arguments);

                    let mut local_dep_execution_time = Duration::new(0, 0);
                    let mut local_execution_time = Duration::new(0, 0);
                    let mut local_checking_time = Duration::new(0, 0);
                    let mut local_apply_write_time = Duration::new(0, 0);

                    let mut local_other_time = Duration::new(0, 0);

                    let mut local_validation_time = Duration::new(0, 0);
                    //let mut local_validation_read_time = Duration::new(0, 0);
                    //let mut local_validation_write_time = Duration::new(0, 0);

                    let mut nothing_to_validate_cnt = 0;
                    let mut nothing_to_execute_cnt = 0;

                    let validate = |version_to_validate: usize, o: bool| -> Option<usize> {
                        let status_to_validate = scheduler.status(version_to_validate);
                        let mut ret = None;

                        // There is a txn to be validated
                        validation_counter.fetch_add(1, Ordering::Relaxed);

                        let state_view = MVHashMapView {
                            map: &versioned_data_cache,
                            version: version_to_validate,
                            scheduler: &scheduler,
                            read_dependency: AtomicBool::new(false),
                            reads: Arc::new(Mutex::new(Vec::new())),
                        };

                        //let read_timer = Instant::now();
                        // If dynamic mv data-structure hasn't been written to, it's safe
                        // to skip validation - it is going to succeed (in order to be
                        // validating, all prior txn's must have been executed already).
                        let valid = versioned_data_cache.dynamic_empty()
                            || status_to_validate.read_set().iter().all(|r| {
                                match state_view.read_map(r.path()) {
                                    Ok((_, version, exec_id)) => {
                                        r.validate(Some((version, exec_id)))
                                    }
                                    Err(Some(_)) => false, //dependency implies validation failure.
                                    Err(None) => r.validate(None),
                                }
                            });
                        //local_validation_read_time += read_timer.elapsed();

                        if !valid && scheduler.abort(version_to_validate, &status_to_validate) {
                            // Not valid && successfully aborted.

                            // Set dirty in both static and dynamic mvhashmaps.
                            //let write_timer = Instant::now();
                            state_view.mark_dirty(
                                version_to_validate,
                                status_to_validate.exec_id(),
                                &infer_result[version_to_validate]
                                    .keys_written
                                    .iter()
                                    .cloned()
                                    .collect(),
                                &status_to_validate.write_set(),
                            );
                            //local_validation_write_time += write_timer.elapsed();

                            ret = scheduler.schedule_txn(version_to_validate, self.num_cpus / 4, o);

                            abort_counter.fetch_add(1, Ordering::Relaxed);
                        } else {
                            scheduler.finish_validation();
                        }

                        ret
                    };

                    let mut other_timer = Instant::now();
                    let mut local_timer;
                    // let loop_timer = Instant::now();
                    loop {
                        if scheduler.done() {
                            local_other_time += other_timer.elapsed();
                            // println!("id = {}, Loop timer = {:?}", id, loop_timer.elapsed());

                            // The work is done, break from the loop.
                            break;
                        }

                        let mut version_to_execute = None;

                        // First check if any txn can be validated
                        let num_validators = validator_workers.load(Ordering::Relaxed);
                        if id < num_validators {
                            if let Some(version_to_validate) = scheduler.next_txn_to_validate() {
                                local_other_time += other_timer.elapsed();

                                local_timer = Instant::now();
                                version_to_execute = validate(version_to_validate, false);
                                local_validation_time += local_timer.elapsed();

                                other_timer = Instant::now();

                                nothing_to_validate_cnt = 0;
                            } else {
                                let mut hint: bool = id > 0; // Don't let 0 wait, may need to set done.
                                if id > 0 && id == num_validators - 1 {
                                    nothing_to_validate_cnt += 1;
                                    // TODO: config parameter
                                    if nothing_to_validate_cnt == NOTHING_ITER_THRESHOLD {
                                        // TODO: is CAS easier to reason about?
                                        validator_workers.fetch_sub(1, Ordering::Relaxed);

                                        nothing_to_validate_cnt = 0;

                                        hint = false;
                                    }
                                }

                                if hint {
                                    hint::spin_loop();
                                }
                            }

                            if version_to_execute.is_none() {
                                // Validation successfully completed or someone already aborted,
                                // continue to the work loop.
                                continue;
                            }
                        }

                        if version_to_execute.is_none() {
                            version_to_execute = scheduler.next_txn_to_execute();
                        }

                        if version_to_execute.is_none() {
                            let mut hint = true;
                            if id < max_validator_workers - 1 && id == num_validators {
                                nothing_to_execute_cnt += 1;

                                if nothing_to_execute_cnt == NOTHING_ITER_THRESHOLD {
                                    // TODO: config parameter
                                    // TODO: is CAS easier to reason about?
                                    validator_workers.fetch_add(1, Ordering::Relaxed);

                                    nothing_to_execute_cnt = 0;
                                    hint = false;
                                }
                            }

                            if hint {
                                hint::spin_loop();
                            }

                            // Nothing to execute, continue to the work loop.
                            continue;
                        } else {
                            nothing_to_execute_cnt = 0;
                        }

                        local_other_time += other_timer.elapsed();
                        local_timer = Instant::now();

                        let txn_to_execute = version_to_execute.unwrap();
                        let txn = &signature_verified_block[txn_to_execute];

                        let txn_accesses = &infer_result[txn_to_execute];

                        let exec_id = scheduler.exec_id(txn_to_execute);
                        if exec_id & 2 == 1 {
                            panic!("executed status while executing");
                        }
                        let has_dependency = if exec_id == 0 {
                            txn_accesses.keys_read.iter().any(|k| {
                                match versioned_data_cache.read(k, txn_to_execute) {
                                    Err(Some((dep_id, _))) => {
                                        scheduler.add_dependency(txn_to_execute, dep_id)
                                    }
                                    Ok(_) | Err(None) => false,
                                }
                            })
                        } else {
                            scheduler.status(txn_to_execute).read_set().iter().any(|r| {
                                match versioned_data_cache.read(r.path(), txn_to_execute) {
                                    Err(Some((dep_id, _))) => {
                                        scheduler.add_dependency(txn_to_execute, dep_id)
                                    }
                                    Ok(_) | Err(None) => false,
                                }
                            })
                        };

                        // If the txn has unresolved dependency, adds the txn to deps_mapping of its dependency (only the first one) and continue
                        if has_dependency {
                            delay_execution_counter.fetch_add(1, Ordering::Relaxed);

                            local_checking_time += local_timer.elapsed();
                            other_timer = Instant::now();

                            continue;
                        }

                        let state_view = MVHashMapView {
                            map: &versioned_data_cache,
                            version: txn_to_execute,
                            scheduler: &scheduler,
                            read_dependency: AtomicBool::new(false),
                            reads: Arc::new(Mutex::new(Vec::new())),
                        };

                        local_checking_time += local_timer.elapsed();
                        local_timer = Instant::now();

                        let execute_result = task.execute_transaction(&state_view, txn);

                        if state_view.read_dependency() {
                            local_dep_execution_time += local_timer.elapsed();
                            other_timer = Instant::now();

                            // We've already added this transaction back to the scheduler in the
                            // MVHashmapView where this bit is set, thus it is safe to continue
                            // here.
                            continue;
                        } else {
                            local_execution_time += local_timer.elapsed();
                            local_timer = Instant::now();
                        }

                        let original_estimates: HashSet<T::Key> =
                            txn_accesses.keys_written.iter().cloned().collect();

                        let mut prev_write_set: HashSet<T::Key> = if exec_id == 0 {
                            original_estimates.clone()
                        } else {
                            scheduler
                                .status(txn_to_execute)
                                .write_set()
                                .iter()
                                .map(|(k, _)| k.clone())
                                .collect()
                        };
                        // prev write set has already been marked dirty! similarly,
                        // the estimates are marked as Unassigned. So future transactions
                        // only need revalidation when there is a write outside of the
                        // prev_write_set.
                        let mut writes_outside = false;

                        let commit_result = match execute_result {
                            ExecutionStatus::Success(output) => {
                                // Commit the side effects to the versioned_data_cache.
                                if output.get_writes().into_iter().all(|(k, v)| {
                                    state_view
                                        .write(
                                            &k,
                                            txn_to_execute,
                                            exec_id,
                                            Some(v),
                                            &original_estimates,
                                            &mut prev_write_set,
                                            &mut writes_outside,
                                        )
                                        .is_ok()
                                }) {
                                    ExecutionStatus::Success(output)
                                } else {
                                    // Failed to write to the versioned data cache as
                                    // transaction write to a key that wasn't estimated by the
                                    // inferencer, aborting the entire execution.
                                    ExecutionStatus::Abort(Error::UnestimatedWrite)
                                }
                            }
                            ExecutionStatus::SkipRest(output) => {
                                // Commit and skip the rest of the transactions.
                                if output.get_writes().into_iter().all(|(k, v)| {
                                    state_view
                                        .write(
                                            &k,
                                            txn_to_execute,
                                            exec_id,
                                            Some(v),
                                            &original_estimates,
                                            &mut prev_write_set,
                                            &mut writes_outside,
                                        )
                                        .is_ok()
                                }) {
                                    scheduler.set_stop_version(txn_to_execute + 1);
                                    ExecutionStatus::SkipRest(output)
                                } else {
                                    // Failed to write to the versioned data cache as
                                    // transaction write to a key that wasn't estimated by the
                                    // inferencer, aborting the entire execution.
                                    ExecutionStatus::Abort(Error::UnestimatedWrite)
                                }
                            }
                            ExecutionStatus::Abort(err) => {
                                // Abort the execution with user defined error.
                                scheduler.set_stop_version(txn_to_execute + 1);
                                ExecutionStatus::Abort(Error::UserError(err.clone()))
                            }
                        };

                        state_view.skip(txn_to_execute, prev_write_set, original_estimates);

                        scheduler.finish_execution(
                            txn_to_execute,
                            exec_id,
                            state_view.drain_reads(),
                            commit_result,
                            writes_outside,
                        );
                        local_apply_write_time += local_timer.elapsed();
                        local_timer = Instant::now();

                        if !writes_outside {
                            if validate(txn_to_execute, true).is_some() {
                                panic!("didn't schedule");
                            }
                            local_validation_time += local_timer.elapsed();
                        }
                        other_timer = Instant::now();
                    }

                    let mut dep_execution = dep_execution_time.lock().unwrap();
                    *dep_execution = max(local_dep_execution_time, *dep_execution);
                    let mut execution = execution_time.lock().unwrap();
                    *execution = max(local_execution_time, *execution);
                    let mut execution_vec = execution_time_vec.lock().unwrap();
                    execution_vec.push((
                        id,
                        local_execution_time.as_millis() + local_dep_execution_time.as_millis(),
                    ));

                    let mut checking = checking_time.lock().unwrap();
                    *checking = max(local_checking_time, *checking);
                    let mut apply_write = apply_write_time.lock().unwrap();
                    *apply_write = max(local_apply_write_time, *apply_write);

                    let mut other = other_time.lock().unwrap();
                    *other = max(local_other_time, *other);
                    let mut other_vec = other_time_vec.lock().unwrap();
                    other_vec.push((id, local_other_time.as_millis()));

                    // let mut validation_read = validation_read_time.lock().unwrap();
                    // *validation_read = max(local_validation_read_time, *validation_read);
                    // let mut validation_write = validation_write_time.lock().unwrap();
                    // *validation_write = max(local_validation_write_time, *validation_write);
                    let mut validation = validation_time.lock().unwrap();
                    *validation = max(local_validation_time, *validation);
                    let mut validation_vec = validation_time_vec.lock().unwrap();
                    validation_vec.push((id, local_validation_time.as_millis()));

                    let mut loop_vec = loop_time_vec.lock().unwrap();
                    loop_vec.push((
                        id,
                        local_dep_execution_time.as_millis()
                            + local_execution_time.as_millis()
                            + local_checking_time.as_millis()
                            + local_other_time.as_millis()
                            + local_validation_time.as_millis()
                            + local_apply_write_time.as_millis(),
                    ));
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
            let compute_cpus = min(1 + (num_txns / 50), self.num_cpus); // Ensure we have at least 50 tx per thread.
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
                                let commit_result = scheduler.output(next_to_commit);
                                // Updates the outcome array
                                outcomes.set_result(next_to_commit, commit_result);
                            }
                            Err(_) => continue,
                        }
                    }
                });
            }
        });

        let set_output_time = timer.elapsed();
        timer = Instant::now();

        println!(
            "Number of execution delays: {}",
            delay_execution_counter.load(Ordering::Relaxed)
        );
        println!(
            "Number of aborts: {}",
            abort_counter.load(Ordering::Relaxed)
        );
        println!(
            "Number of validations: {}",
            validation_counter.load(Ordering::Relaxed)
        );

        let ((s_max, s_avg), (d_max, d_avg)) = versioned_data_cache.get_depth();
        println!("Static mvhashmap: max depth {}, avg depth {}", s_max, s_avg);
        println!(
            "Dynamic mvhashmap: max depth {}, avg depth {}\n",
            d_max, d_avg
        );

        let ret = outcomes.get_all_results(valid_results_length);

        ::std::thread::spawn(move || {
            drop(infer_result);
            drop(signature_verified_block); // Explicit drops to measure their cost.
            drop(versioned_data_cache);

            drop(scheduler);
        });

        let remaining_time = timer.elapsed();
        let total_time = timer_start.elapsed();

        println!(
            "=====PERF=====\n\
                mvhashmap_construction_time {:?},\n\
                execution_loop_time {:?},\n\
                set_output_time {:?},\n\
                remaining_time {:?},\n\
                total_time {:?}\n",
            mvhashmap_construction_time,
            execution_loop_time,
            set_output_time,
            remaining_time,
            total_time
        );

        println!(
            "=====INSIDE THE LOOP (max among all threads)=====,\n\
                dep_execution_time (till read error) {:?},\n\
                execution_time (no read error) {:?},\n\
                apply_write_time {:?},\n\
                read_pre_checking_time {:?},\n\
                validation_time {:?},\n\
                other_time (incl get next val or exec) {:?}\n",
            dep_execution_time.lock().unwrap(),
            execution_time.lock().unwrap(),
            apply_write_time.lock().unwrap(),
            checking_time.lock().unwrap(),
            validation_time.lock().unwrap(),
            //validation_read_time.lock().unwrap(),
            //validation_write_time.lock().unwrap(),
            other_time.lock().unwrap(),
        );

        execution_time_vec.lock().unwrap().sort();
        validation_time_vec.lock().unwrap().sort();
        other_time_vec.lock().unwrap().sort();
        loop_time_vec.lock().unwrap().sort();

        println!(
            "=====INSIDE THE LOOP (vec with thread IDs)=====,\n\
               execution_time {:?},\n\
               validation_time {:?},\n\
               other_time {:?},\n\
               loop_time {:?}\n",
            execution_time_vec.lock().unwrap(),
            validation_time_vec.lock().unwrap(),
            other_time_vec.lock().unwrap(),
            loop_time_vec.lock().unwrap(),
        );

        ret
    }
}
