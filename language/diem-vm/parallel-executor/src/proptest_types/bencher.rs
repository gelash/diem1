// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    executor::ParallelTransactionExecutor,
    proptest_types::types::{
        ExpectedOutput, STMInferencer, Task, Transaction, TransactionGen, TransactionGenParams,
    },
};
use criterion::{BatchSize, Bencher as CBencher};
use proptest::{
    arbitrary::Arbitrary,
    collection::vec,
    prelude::*,
    strategy::{Strategy, ValueTree},
    test_runner::TestRunner,
};

use std::{fmt::Debug, hash::Hash, marker::PhantomData};

pub struct Bencher<K, V> {
    transaction_size: usize,
    transaction_gen_param: TransactionGenParams,
    universe_size: usize,
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
    drop_write: f64,
    drop_read: f64,
}

pub(crate) struct BencherState<K, V> {
    transactions: Vec<Transaction<K, V>>,
    expected_output: Option<ExpectedOutput<V>>,
    drop_write: f64,
    drop_read: f64,
}

impl<K, V> Bencher<K, V>
where
    K: Hash + Clone + Debug + Eq + Send + Sync + PartialOrd + Ord + Arbitrary + 'static,
    V: Clone + Eq + Send + Sync + Arbitrary + 'static,
{
    pub fn new(
        transaction_size: usize,
        universe_size: usize,
        drop_write: f64,
        drop_read: f64,
    ) -> Self {
        Self {
            transaction_size,
            transaction_gen_param: TransactionGenParams::default(),
            universe_size,
            phantom_key: PhantomData,
            phantom_value: PhantomData,
            drop_write,
            drop_read,
        }
    }

    pub fn bench(&self, key_strategy: &impl Strategy<Value = K>, bencher: &mut CBencher) {
        bencher.iter_batched(
            || {
                BencherState::<K, V>::with_universe(
                    vec(key_strategy, self.universe_size),
                    self.transaction_size,
                    self.transaction_gen_param,
                    true,
                    self.drop_write,
                    self.drop_read,
                )
            },
            |state| state.run(),
            // The input here is the entire list of signed transactions, so it's pretty large.
            BatchSize::LargeInput,
        )
    }
}

impl<K, V> BencherState<K, V>
where
    K: Hash + Clone + Debug + Eq + Send + Sync + PartialOrd + Ord + 'static,
    V: Clone + Eq + Send + Sync + Arbitrary + 'static,
{
    /// Creates a new benchmark state with the given account universe strategy and number of
    /// transactions.
    pub(crate) fn with_universe(
        universe_strategy: impl Strategy<Value = Vec<K>>,
        num_transactions: usize,
        transaction_params: TransactionGenParams,
        check_correctness: bool,
        drop_write: f64,
        drop_read: f64,
    ) -> Self {
        let mut runner = TestRunner::default();
        let key_universe = universe_strategy
            .new_tree(&mut runner)
            .expect("creating a new value should succeed")
            .current();

        let transaction_gens = vec(
            any_with::<TransactionGen<V>>(transaction_params),
            num_transactions,
        )
        .new_tree(&mut runner)
        .expect("creating a new value should succeed")
        .current();

        let transactions: Vec<_> = transaction_gens
            .into_iter()
            .map(|txn_gen| txn_gen.materialize(&key_universe))
            .collect();

        let expected_output = if check_correctness {
            Some(ExpectedOutput::generate_baseline(&transactions))
        } else {
            None
        };

        Self {
            transactions,
            expected_output,
            drop_write,
            drop_read,
        }
    }

    pub(crate) fn run(self) {
        // let txns = self.transactions.clone();
        // for id in 0..txns.len() {
        //     println!("id {}", id);
        //     if let Transaction::Write { actual_writes, skipped_writes, reads } = &txns[id] {
        //         println!("actual writes {:?}", actual_writes);
        //         println!("skipped writes {:?}", skipped_writes);
        //         println!("reads {:?}\n", reads);
        //     }
        // }

        let output =
            ParallelTransactionExecutor::<Transaction<K, V>, Task<K, V>, STMInferencer<K, V>>::new(
                STMInferencer::new(self.drop_write, self.drop_read),
            )
            .execute_transactions_parallel((), self.transactions);

        if let Some(expected_output) = self.expected_output {
            assert!(expected_output.check_output(&output));
            println!("Parallel execution same as sequential execution!");
        }
    }
}
