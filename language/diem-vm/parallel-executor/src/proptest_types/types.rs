// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    errors::{Error, Result},
    executor::MVHashMapView,
    task::{
        Accesses, ExecutionStatus, ExecutorTask, ReadWriteSetInferencer,
        Transaction as TransactionType, TransactionOutput,
    },
};
use anyhow::Result as AResult;
// use diem_types::access_path::AccessPath;
use proptest::{
    arbitrary::Arbitrary, collection::vec, prelude::*, proptest, sample::Index, strategy::Strategy,
};
use proptest_derive::Arbitrary;
use rand::Rng;
use std::{
    collections::{BTreeSet, HashMap},
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
};

///////////////////////////////////////////////////////////////////////////
// Generation of transactions
///////////////////////////////////////////////////////////////////////////

#[derive(Arbitrary, Debug, Clone)]
#[proptest(params = "TransactionGenParams")]
pub struct TransactionGen<V: Arbitrary + Debug + 'static + Clone> {
    #[proptest(
        strategy = "vec((any::<Index>(), value_strategy(params.write_keep_rate)), 1..params.possible_write_size)"
    )]
    keys_modified: Vec<(Index, Option<V>)>,
    #[proptest(strategy = "vec(any::<Index>(), 1..params.read_size)")]
    keys_read: Vec<Index>,
}

#[derive(Clone, Copy)]
pub struct TransactionGenParams {
    pub possible_write_size: usize,
    pub read_size: usize,
    pub write_keep_rate: f64,
}

/// A naive transaction that could be used to test the correctness and throughput of the system.
#[derive(Debug, Clone)]
pub enum Transaction<K, V> {
    Write {
        /// Write to some keys with value provided.
        actual_writes: Vec<(K, V)>,
        /// Skipp writing to some keys. This is used to simulate over approximation of the inferencer.
        skipped_writes: Vec<K>,
        /// Read from some keys.
        reads: Vec<K>,
    },
    /// Skip the execution of trailing transactions.
    SkipRest,
    /// Abort the execution.
    Abort,
}

impl Default for TransactionGenParams {
    fn default() -> Self {
        TransactionGenParams {
            possible_write_size: 10,
            write_keep_rate: 0.5,
            read_size: 10,
        }
    }
}

fn value_strategy<V: Arbitrary + Clone + 'static>(
    keep_rate: f64,
) -> impl Strategy<Value = Option<V>> {
    let value_strategy = any::<V>();
    proptest::option::weighted(keep_rate, value_strategy)
}

impl<V: Arbitrary + Debug + Clone> TransactionGen<V> {
    pub fn materialize<K: Clone + Eq + Ord>(self, universe: &[K]) -> Transaction<K, V> {
        let mut keys_modified = BTreeSet::new();
        let mut actual_writes = vec![];
        let mut skipped_writes = vec![];
        for (idx, value) in self.keys_modified.into_iter() {
            let key = universe[idx.index(universe.len())].clone();
            if !keys_modified.contains(&key) {
                keys_modified.insert(key.clone());
                match value {
                    None => skipped_writes.push(key),
                    Some(v) => actual_writes.push((key, v)),
                };
            }
        }
        Transaction::Write {
            actual_writes,
            skipped_writes,
            reads: self
                .keys_read
                .into_iter()
                .map(|k| universe[k.index(universe.len())].clone())
                .collect(),
        }
    }
}

impl<K, V> TransactionType for Transaction<K, V>
where
    K: PartialOrd + Send + Sync + Clone + Hash + Eq + 'static,
    V: Send + Sync + Debug + Clone + 'static,
{
    type Key = K;
    type Value = V;
}

///////////////////////////////////////////////////////////////////////////
// Naive inferencer implementation.
///////////////////////////////////////////////////////////////////////////

pub struct Inferencer<K, V>(PhantomData<(K, V)>);

impl<K, V> Inferencer<K, V> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<K, V> ReadWriteSetInferencer for Inferencer<K, V>
where
    K: PartialOrd + Send + Sync + Clone + Hash + Eq + 'static,
    V: Send + Sync + Debug + Clone + 'static,
{
    type T = Transaction<K, V>;

    fn infer_reads_writes(&self, txn: &Self::T) -> AResult<Accesses<K>> {
        match txn {
            Transaction::Write {
                actual_writes,
                skipped_writes,
                reads,
            } => {
                let mut writes = actual_writes
                    .iter()
                    .map(|(k, _)| k.clone())
                    .collect::<Vec<_>>();
                writes.append(&mut skipped_writes.clone());
                Ok(Accesses {
                    keys_read: reads.clone(),
                    keys_written: writes,
                })
            }
            Transaction::SkipRest | Transaction::Abort => Ok(Accesses {
                keys_read: vec![],
                keys_written: vec![],
            }),
        }
    }

    fn infer_results(
        &mut self,
        _block: &Vec<Self::T>,
        _write_keep_rate: f32,
        _read_keep_rate: f32,
    ) -> usize {
        // For this one dropping estimates is implemented elsewhere.
        0
    }

    fn result(&self, block: &Vec<Self::T>) -> Vec<Accesses<K>> {
        match block
            .iter()
            .map(|txn| self.infer_reads_writes(txn))
            .collect::<AResult<Vec<_>>>()
        {
            Ok(res) => res,
            // Inferencer passed in by user failed to get the read/writeset of a transaction,
            // abort parallel execution.
            Err(_) => panic!(),
        }
    }
}

pub struct ImpreciseInferencer<K, V>(PhantomData<(K, V)>);

impl<K, V> ImpreciseInferencer<K, V> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<K, V> ReadWriteSetInferencer for ImpreciseInferencer<K, V>
where
    K: PartialOrd + Send + Sync + Clone + Hash + Eq + 'static,
    V: Send + Sync + Debug + Clone + 'static,
{
    type T = Transaction<K, V>;

    fn infer_reads_writes(&self, txn: &Self::T) -> AResult<Accesses<K>> {
        match txn {
            Transaction::Write {
                actual_writes,
                skipped_writes,
                reads,
            } => {
                let mut writes = actual_writes
                    .iter()
                    .map(|(k, _)| k.clone())
                    .collect::<Vec<_>>();
                writes.append(&mut skipped_writes.clone());

                let mut reads_result = reads.clone();
                // Drop one read entry to simulate imprecise read estimation
                reads_result.pop();

                Ok(Accesses {
                    keys_read: reads_result,
                    keys_written: writes,
                })
            }
            Transaction::SkipRest | Transaction::Abort => Ok(Accesses {
                keys_read: vec![],
                keys_written: vec![],
            }),
        }
    }

    fn infer_results(
        &mut self,
        _block: &Vec<Self::T>,
        _write_keep_rate: f32,
        _read_keep_rate: f32,
    ) -> usize {
        // For this one dropping estimates is implemented elsewhere.
        0
    }

    fn result(&self, block: &Vec<Self::T>) -> Vec<Accesses<K>> {
        match block
            .iter()
            .map(|txn| self.infer_reads_writes(txn))
            .collect::<AResult<Vec<_>>>()
        {
            Ok(res) => res,
            // Inferencer passed in by user failed to get the read/writeset of a transaction,
            // abort parallel execution.
            Err(_) => panic!(),
        }
    }
}

pub struct STMInferencer<K, V>(PhantomData<(K, V)>, f64, f64);

impl<K, V> STMInferencer<K, V> {
    pub fn new(drop_write: f64, drop_read: f64) -> Self {
        Self(PhantomData, drop_write, drop_read)
    }
}

impl<K, V> ReadWriteSetInferencer for STMInferencer<K, V>
where
    K: PartialOrd + Send + Sync + Clone + Hash + Eq + 'static,
    V: Send + Sync + Debug + Clone + 'static,
{
    type T = Transaction<K, V>;

    fn infer_reads_writes(&self, txn: &Self::T) -> AResult<Accesses<K>> {
        match txn {
            Transaction::Write {
                actual_writes,
                skipped_writes,
                reads,
            } => {
                let mut writes = actual_writes
                    .iter()
                    .map(|(k, _)| k.clone())
                    .collect::<Vec<_>>();
                writes.append(&mut skipped_writes.clone());

                let mut writes_result = Vec::new();
                let mut reads_result = Vec::new();
                let mut rng = rand::thread_rng(); // randomness

                for write in writes {
                    if rng.gen_range(0.0..10.0) / 10.0 > self.1 {
                        writes_result.push(write.clone());
                    }
                }

                for read in reads {
                    if rng.gen_range(0.0..10.0) / 10.0 > self.2 {
                        reads_result.push(read.clone());
                    }
                }

                Ok(Accesses {
                    keys_read: reads_result,
                    keys_written: writes_result,
                })
            }
            Transaction::SkipRest | Transaction::Abort => Ok(Accesses {
                keys_read: vec![],
                keys_written: vec![],
            }),
        }
    }

    fn infer_results(
        &mut self,
        _block: &Vec<Self::T>,
        _write_keep_rate: f32,
        _read_keep_rate: f32,
    ) -> usize {
        // For this one dropping estimates is implemented elsewhere.
        0
    }

    fn result(&self, block: &Vec<Self::T>) -> Vec<Accesses<K>> {
        match block
            .iter()
            .map(|txn| self.infer_reads_writes(txn))
            .collect::<AResult<Vec<_>>>()
        {
            Ok(res) => res,
            // Inferencer passed in by user failed to get the read/writeset of a transaction,
            // abort parallel execution.
            Err(_) => panic!(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////
// Naive transaction executor implementation.
///////////////////////////////////////////////////////////////////////////

pub struct Task<K, V>(PhantomData<(K, V)>);

impl<K, V> Task<K, V> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<K, V> ExecutorTask for Task<K, V>
where
    K: PartialOrd + Send + Sync + Clone + Hash + Eq + 'static,
    V: Send + Sync + Debug + Clone + 'static,
{
    type T = Transaction<K, V>;
    type Output = Output<K, V>;
    type Error = usize;
    type Argument = ();

    fn init(_argument: Self::Argument) -> Self {
        Self::new()
    }

    fn execute_transaction(
        &self,
        view: &MVHashMapView<K, V, Self::Output, Self::Error>,
        txn: &Self::T,
    ) -> ExecutionStatus<Self::Output, Self::Error> {
        match txn {
            Transaction::Write {
                reads,
                actual_writes,
                skipped_writes: _,
            } => {
                // Reads
                let mut reads_result = vec![];
                for k in reads.iter() {
                    reads_result.push(match view.read(k) {
                        Ok(Some(v)) => Some(v.clone()),
                        Ok(None) => None,
                        Err(_) => return ExecutionStatus::Abort(0),
                    })
                }
                ExecutionStatus::Success(Output(actual_writes.clone(), reads_result))
            }
            Transaction::SkipRest => ExecutionStatus::SkipRest(Output(vec![], vec![])),
            Transaction::Abort => ExecutionStatus::Abort(view.version()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Output<K, V>(Vec<(K, V)>, Vec<Option<V>>);

impl<K, V> TransactionOutput for Output<K, V>
where
    K: PartialOrd + Send + Sync + Clone + Hash + Eq + 'static,
    V: Send + Sync + Debug + Clone + 'static,
{
    type T = Transaction<K, V>;

    fn get_writes(&self) -> Vec<(K, V)> {
        self.0.clone()
    }

    fn skip_output() -> Self {
        Self(vec![], vec![])
    }
}

///////////////////////////////////////////////////////////////////////////
// Sequential Baseline implementation.
///////////////////////////////////////////////////////////////////////////

/// Sequential baseline of execution result for dummy transaction.
#[derive(Debug)]
pub enum ExpectedOutput<V> {
    Aborted(usize),
    SkipRest(usize, Vec<Vec<Option<V>>>),
    Success(Vec<Vec<Option<V>>>),
}

impl<V: Clone + Eq + Debug> ExpectedOutput<V> {
    pub fn generate_baseline<K: Hash + Clone + Eq>(txns: &[Transaction<K, V>]) -> Self {
        let mut current_world = HashMap::new();
        let mut result_vec = vec![];
        for (idx, txn) in txns.iter().enumerate() {
            match txn {
                Transaction::Abort => return Self::Aborted(idx),
                Transaction::Write {
                    reads,
                    actual_writes,
                    skipped_writes: _,
                } => {
                    let mut result = vec![];
                    for k in reads.iter() {
                        result.push(current_world.get(k).cloned());
                    }
                    for (k, v) in actual_writes.iter() {
                        current_world.insert(k.clone(), v.clone());
                    }
                    result_vec.push(result)
                }
                Transaction::SkipRest => return Self::SkipRest(idx, result_vec),
            }
        }
        Self::Success(result_vec)
    }

    pub fn check_output<K>(&self, results: &Result<Vec<Output<K, V>>, usize>) -> bool {
        match (self, results) {
            (Self::Aborted(i), Err(Error::UserError(idx))) => i == idx,
            (Self::SkipRest(skip_at, expected_results), Ok(results)) => {
                results
                    .iter()
                    .take(*skip_at)
                    .zip(expected_results.iter())
                    .all(|(Output(_, result), expected_results)| expected_results == result)
                    && results
                        .iter()
                        .skip(*skip_at)
                        .all(|Output(_, result)| result.is_empty())
            }
            (Self::Success(expected_results), Ok(results)) => {
                for id in 0..expected_results.len() {
                    let expected_result = &expected_results[id];
                    let Output(_, result) = &results[id];
                    if expected_result != result {
                        println!("Transaction id {}", id);
                        println!("expect: {:?}", expected_result);
                        println!("result: {:?}\n", result);
                        return false;
                    }
                }
                return true;
            }
            // expected_results
            // .iter()
            // .zip(results.iter())
            // .all(|(expected_result, Output(_, result))| {
            //     if expected_result == result {
            //         return true;
            //     } else {
            //         println!("expect: {:?}", expected_result);
            //         println!("result: {:?}\n", result);
            //         return false;
            //     }
            //     // return expected_result == result;
            // }),
            _ => false,
        }
    }
}
