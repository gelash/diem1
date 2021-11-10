// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

mod read_write_set_analyzer;
mod storage_wrapper;
mod vm_wrapper;

use crate::{
    adapter_common::{preprocess_transaction, PreprocessedTransaction},
    data_cache::RemoteStorage,
    diem_vm::DiemVM,
    parallel_executor::{
        read_write_set_analyzer::ReadWriteSetAnalysisWrapper, vm_wrapper::DiemVMWrapper,
    },
    VMExecutor,
};
use diem_parallel_executor::{
    errors::Error,
    executor::ParallelTransactionExecutor,
    task::{
        ReadWriteSetInferencer, Transaction as PTransaction,
        TransactionOutput as PTransactionOutput,
    },
};
use diem_state_view::StateView;
use diem_types::{
    access_path::AccessPath,
    transaction::{Transaction, TransactionOutput, TransactionStatus},
    write_set::{WriteOp, WriteSet},
};
use move_core_types::vm_status::{StatusCode, VMStatus};
use rayon::prelude::*;
use read_write_set_dynamic::NormalizedReadWriteSetAnalysis;
use std::time::{Duration, Instant};

impl PTransaction for PreprocessedTransaction {
    type Key = AccessPath;
    type Value = WriteOp;
}

// Wrapper to avoid orphan rule
pub(crate) struct DiemTransactionOutput(TransactionOutput);

impl DiemTransactionOutput {
    pub fn new(output: TransactionOutput) -> Self {
        Self(output)
    }
    pub fn into(self) -> TransactionOutput {
        self.0
    }
}

impl PTransactionOutput for DiemTransactionOutput {
    type T = PreprocessedTransaction;

    fn get_writes(&self) -> Vec<(AccessPath, WriteOp)> {
        self.0.write_set().iter().cloned().collect()
    }

    /// Execution output for transactions that comes after SkipRest signal.
    fn skip_output() -> Self {
        Self(TransactionOutput::new(
            WriteSet::default(),
            vec![],
            0,
            TransactionStatus::Retry,
        ))
    }
}

pub struct ParallelDiemVM();

impl ParallelDiemVM {
    pub fn execute_block<S: StateView>(
        analysis_result: &NormalizedReadWriteSetAnalysis,
        transactions: Vec<Transaction>,
        state_view: &S,
    ) -> Result<(Vec<TransactionOutput>, Option<Error<VMStatus>>), VMStatus> {
        let blockchain_view = RemoteStorage::new(state_view);
        let mut analyzer = ReadWriteSetAnalysisWrapper::new(analysis_result, &blockchain_view);

        // Verify the signatures of all the transactions in parallel.
        // This is time consuming so don't wait and do the checking
        // sequentially while executing the transactions.

        // let mut timer = Instant::now();
        let signature_verified_block: Vec<PreprocessedTransaction> = transactions
            .par_iter()
            .map(|txn| preprocess_transaction::<DiemVM>(txn.clone()))
            .collect();
        // println!("CLONE & Prologue {:?}", timer.elapsed());

        analyzer.infer_results(&signature_verified_block, 1.0);

        let executor = ParallelTransactionExecutor::<
            PreprocessedTransaction,
            DiemVMWrapper<S>,
            ReadWriteSetAnalysisWrapper<RemoteStorage<S>>,
        >::new(analyzer);

        let ret = match executor.execute_transactions_parallel(state_view, signature_verified_block)
        {
            Ok(results) => {
                Ok((
                    results
                        .into_iter()
                        .map(DiemTransactionOutput::into)
                        .collect(),
                    None,
                ))
                // let timer1 = Instant::now();
                // drop(transactions);
                // println!("DROP TXN {:?}", timer1.elapsed());
            }
            Err(err @ Error::InferencerError) | Err(err @ Error::UnestimatedWrite) => {
                panic!();
                // Ok((DiemVM::execute_block(transactions, state_view)?, Some(err)))
            }
            Err(Error::InvariantViolation) => Err(VMStatus::Error(
                StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR,
            )),
            Err(Error::UserError(err)) => Err(err),
        };
        // drop(executor);
        // drop(blockchain_view);
        // println!("PARALLEL EXECUTE OUTSIDE {:?}", timer.elapsed());

        ret
    }

    pub fn execute_block_timer<S: StateView>(
        analysis_result: &NormalizedReadWriteSetAnalysis,
        transactions: Vec<Transaction>,
        state_view: &S,
        write_keep_rate: f32,
    ) -> (usize, usize) {
        let blockchain_view = RemoteStorage::new(state_view);
        let mut analyzer = ReadWriteSetAnalysisWrapper::new(analysis_result, &blockchain_view);

        // Verify the signatures of all the transactions in parallel.
        // This is time consuming so don't wait and do the checking
        // sequentially while executing the transactions.

        // let mut timer = Instant::now();
        let signature_verified_block: Vec<PreprocessedTransaction> = transactions
            .par_iter()
            .map(|txn| preprocess_transaction::<DiemVM>(txn.clone()))
            .collect();
        // println!("CLONE & Prologue {:?}", timer.elapsed());

        let analysis_time = analyzer.infer_results(&signature_verified_block, write_keep_rate);

        let executor = ParallelTransactionExecutor::<
            PreprocessedTransaction,
            DiemVMWrapper<S>,
            ReadWriteSetAnalysisWrapper<RemoteStorage<S>>,
        >::new(analyzer);

        let timer = Instant::now();
        let useless = executor.execute_transactions_parallel(state_view, signature_verified_block);
        let exec_t = timer.elapsed();

        drop(useless);

        (exec_t.as_millis() as usize, analysis_time)
    }
}
