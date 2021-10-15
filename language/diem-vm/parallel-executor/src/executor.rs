// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    errors::*,
    outcome_array::OutcomeArray,
    scheduler::{Scheduler, ReadDescriptor},
    task::{ExecutionStatus, ExecutorTask, ReadWriteSetInferencer, Transaction, TransactionOutput},
};
use anyhow::{bail, Result as AResult};
use mvhashmap::{MVHashMap, Version};
use num_cpus;
use rayon::{prelude::*, scope};
use std::{
    cmp::{max, min},
    hash::Hash,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    collections::HashMap,
};

pub struct MVHashMapView<'a, K, V, T, E> {
    map: &'a MVHashMap<K, V>,
    version: Version,
    scheduler: &'a Scheduler<K, T, E>,
    has_unexpected_read: AtomicBool,
    reads: Arc<RwLock<HashMap<K, Option<(usize, usize)>>>>,
}

impl<'a, K: PartialOrd + Send + Clone + Hash + Eq, V: Clone + Send + Sync, T: TransactionOutput, E: Send + Clone> MVHashMapView<'a, K, V, T, E> {
    // Drains the reads.
    pub fn drain_reads(&self) -> Vec<ReadDescriptor<K>> {
        self.reads
            .write()
            .unwrap()
            .drain()
            .map(|(path, version_and_retry_num)| {
                ReadDescriptor::new(path, version_and_retry_num)
            })
            .collect()
    }
    
    pub fn read(&self, key: &K) -> AResult<Option<V>> {
        match self.map.read(key, self.version) {
            Ok((v, version, retry_num)) => {
                self.reads.write().unwrap().insert(key.clone(), Some((version, retry_num)));
                return Ok(v);
            }
            Err(None) => {
                self.reads.write().unwrap().insert(key.clone(), None);
                return Ok(None);
            }
            Err(Some((dep_idx, retry_num))) => {
                // Don't start execution transaction `self.version` until `dep_idx` is computed.
                if !self.scheduler.add_dependency(self.version, dep_idx) {
                    // dep_idx is already executed, push `self.version` to ready queue.
                    self.scheduler.add_transaction(self.version);
                }
                self.has_unexpected_read.fetch_or(true, Ordering::Relaxed);
                bail!("Read dependency is not computed, retry later")
            }
        }
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn has_unexpected_read(&self) -> bool {
        self.has_unexpected_read.load(Ordering::Relaxed)
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
        let num_txns = signature_verified_block.len();
        let chunks_size = max(1, num_txns / self.num_cpus);

        // Get the read and write dependency for each transaction.
        let infer_result: Vec<_> = {
            match signature_verified_block
                .par_iter()
                .with_min_len(chunks_size)
                .map(|txn| self.inferencer.infer_reads_writes(txn))
                .collect::<AResult<Vec<_>>>()
            {
                Ok(res) => res,
                // Inferencer passed in by user failed to get the read/writeset of a transaction,
                // abort parallel execution.
                Err(_) => return Err(Error::InferencerError),
            }
        };

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
        let outcomes = OutcomeArray::new(num_txns);

        let scheduler = Arc::new(Scheduler::new(num_txns));

        scope(|s| {
            // How many threads to use?
            let compute_cpus = min(1 + (num_txns / 50), self.num_cpus - 1); // Ensure we have at least 50 tx per thread.
            let compute_cpus = min(num_txns / max_dependency_level, compute_cpus); // Ensure we do not higher rate of conflict than concurrency.

            println!(
                "Launching {} threads to execute (Max conflict {}) ... total txns: {:?}",
                compute_cpus,
                max_dependency_level,
                scheduler.num_txn_to_execute(),
            );
            for _ in 0..(compute_cpus) {
                s.spawn(|_| {
                    let scheduler = Arc::clone(&scheduler);
                    // Make a new executor per thread.
                    let task = E::init(task_initial_arguments);

                    while let Some(idx) = scheduler.next_txn_to_execute() {
                        let txn = &signature_verified_block[idx];
                        let txn_accesses = &infer_result[idx];

                        // If the txn has unresolved dependency, adds the txn to deps_mapping of its dependency (only the first one) and continue
                        if txn_accesses.keys_read.iter().any(|k| {
                            match versioned_data_cache.read_from_static(k, idx) {
                                Err(Some((dep_id, _))) => scheduler.add_dependency(idx, dep_id),
                                Ok(_) | Err(None) => false,
                            }
                        }) {
                            // This causes a PAUSE on an x64 arch, and takes 140 cycles. Allows other
                            // core to take resources and better HT.
                            ::std::hint::spin_loop();
                            continue;
                        }

                        // Process the output of a transaction
                        let view = MVHashMapView {
                            map: &versioned_data_cache,
                            version: idx,
                            scheduler: &scheduler,
                            has_unexpected_read: AtomicBool::new(false),
                            reads: Arc::new(RwLock::new(HashMap::new())),
                        };
                        let execute_result = task.execute_transaction(&view, txn);
                        if view.has_unexpected_read() {
                            // We've already added this transaction back to the scheduler in the
                            // MVHashmapView where this bit is set, thus it is safe to continue
                            // here.
                            continue;
                        }
                        
                        let retry_num = scheduler.retry_num(idx);

                        let commit_result =
                            match execute_result {
                                ExecutionStatus::Success(output) => {
                                    // Commit the side effects to the versioned_data_cache.
                                    if output.get_writes().into_iter().all(|(k, v)| {
                                        versioned_data_cache.write_to_static(&k, idx, retry_num, Some(v)).is_ok()
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
                                        versioned_data_cache.write_to_static(&k, idx, retry_num, Some(v)).is_ok()
                                    }) {
                                        scheduler.set_stop_version(idx + 1);
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
                                    scheduler.set_stop_version(idx + 1);
                                    ExecutionStatus::Abort(Error::UserError(err.clone()))
                                }
                            };

                        for write in txn_accesses.keys_written.iter() {
                            // Unwrap here is fine because all writes here should be in the mvhashmap.
                            assert!(versioned_data_cache.skip_if_not_set(write, idx, retry_num).is_ok());
                        }

                        scheduler.finish_execution(idx, retry_num, view.drain_reads(), commit_result.clone());
                        outcomes.set_result(idx, commit_result);
                    }
                });
            }
        });

        // Splits the head of the vec of results that are valid
        let valid_results_length = scheduler.num_txn_to_execute();

        // // Dropping large structures is expensive -- do this is a separate thread.
        // ::std::thread::spawn(move || {
        //     drop(scheduler);
        //     drop(infer_result);
        //     drop(signature_verified_block); // Explicit drops to measure their cost.
        //     drop(versioned_data_cache);
        // });

        outcomes.get_all_results(valid_results_length)
    }
}
