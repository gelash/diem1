// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    adapter_common::PreprocessedTransaction, read_write_set_analysis::ReadWriteSetAnalysis,
};
use anyhow::Result;
use diem_parallel_executor::task::{Accesses, ReadWriteSetInferencer};
use diem_types::access_path::AccessPath;
use move_core_types::resolver::MoveResolver;
use num_cpus;
use rand::Rng;
use rayon::prelude::*;
use read_write_set_dynamic::NormalizedReadWriteSetAnalysis;
use std::{cell::RefCell, cmp::max, sync::Mutex, time::Instant};

pub(crate) struct ReadWriteSetAnalysisWrapper<'a, S: MoveResolver> {
    analyzer: ReadWriteSetAnalysis<'a, S>,
    results: Mutex<RefCell<Vec<Accesses<AccessPath>>>>,
}

impl<'a, S: MoveResolver> ReadWriteSetAnalysisWrapper<'a, S> {
    pub fn new(analysis_result: &'a NormalizedReadWriteSetAnalysis, view: &'a S) -> Self {
        Self {
            analyzer: ReadWriteSetAnalysis::new(analysis_result, view),
            results: Mutex::new(RefCell::new(Vec::new())),
        }
    }
}

impl<'a, S: MoveResolver + std::marker::Sync> ReadWriteSetInferencer
    for ReadWriteSetAnalysisWrapper<'a, S>
{
    type T = PreprocessedTransaction;
    fn infer_reads_writes(&self, txn: &Self::T) -> Result<Accesses<AccessPath>> {
        let (keys_read, keys_written) = self.analyzer.get_keys_transaction(txn, false)?;
        Ok(Accesses {
            keys_read: keys_read
                .into_iter()
                .map(AccessPath::resource_access_path)
                .collect(),
            keys_written: keys_written
                .into_iter()
                .map(AccessPath::resource_access_path)
                .collect(),
        })
    }

    fn infer_results(&mut self, block: &Vec<Self::T>, write_keep_rate: f32) -> usize {
        let num_txns = block.len();
        let chunks_size = max(1, num_txns / num_cpus::get());

        let timer = Instant::now();
        // Get the read and write dependency for each transaction.
        let mut estimates = {
            match block
                .par_iter()
                .with_min_len(chunks_size)
                .map(|txn| self.infer_reads_writes(txn))
                .collect::<Result<Vec<_>>>()
            {
                Ok(res) => res,
                // Inferencer passed in by user failed to get the read/writeset of a transaction,
                // abort parallel execution.
                Err(_) => panic!(),
            }
        };
        let t = timer.elapsed();

        let mut rng = rand::thread_rng();
        for e in &mut estimates {
            e.keys_written
                .retain(|_| rng.gen_range(0.0..10.0) / 10.0 <= write_keep_rate)
        }

        self.results.lock().unwrap().replace(estimates);

        t.as_millis() as usize
    }

    fn result(&self, _block: &Vec<Self::T>) -> Vec<Accesses<AccessPath>> {
        self.results.lock().unwrap().replace(Vec::new())
        // std::mem::take(&mut self.results)
    }
}
