// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0
use crate::{
    errors::Error,
    task::{ExecutionStatus, Transaction, TransactionOutput},
};
use arc_swap::ArcSwapOption;
use crossbeam::utils::CachePadded;
use crossbeam_queue::SegQueue;
use mvhashmap::Version;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex, RwLock,
};

const MASK: u64 = ((1 as u64) << 32) - 1;

#[derive(Clone)]
pub struct ReadDescriptor<K> {
    access_path: K,

    // Is set to (version, exec_id) if the read was from shared MV data-structure
    // (written by a previous txn), None if read from storage.
    version_and_exec_id: Option<(usize, usize)>, // TODO: Version alias for usize.
}

impl<K> ReadDescriptor<K> {
    pub fn new(access_path: K, version_and_exec_id: Option<(usize, usize)>) -> Self {
        Self {
            access_path,
            version_and_exec_id,
        }
    }

    pub fn path(&self) -> &K {
        &self.access_path
    }

    pub fn validate(&self, new_version_and_exec_id: Option<(usize, usize)>) -> bool {
        self.version_and_exec_id == new_version_and_exec_id
    }
}

// Transaction status. When the transaction is first executed, new STMStatus
// w. exec_id = 1 and the corresponding input/ouput is stored.
pub struct STMStatus<K, T, E> {
    // Unique identifier for each (re-)execution. Guaranteed to be add, with first execution
    // having exec_id = 1, second re-execution having exec_id = 3, etc.
    exec_id: usize,

    // Transactions input/output set from the last execution (for all actual reads and writes),
    // plus a bit to (test-and-set) synchronize among failed validators (so only one can abort).
    input: Vec<ReadDescriptor<K>>,
    // Can take once.
    output: RwLock<Option<ExecutionStatus<T, Error<E>>>>,
}

impl<K, T: TransactionOutput, E: Send + Clone> STMStatus<K, T, E> {
    pub fn exec_id(&self) -> usize {
        self.exec_id
    }

    // executed() must hold, i.e. input_output != None. Returns a reference to the inner Vec.
    pub fn read_set(&self) -> &Vec<ReadDescriptor<K>> {
        &self.input
    }

    // executed() must hold, i.e. input_output != None. Returns a reference to the inner writeset.
    pub fn write_set(
        &self,
    ) -> Vec<(
        <<T as TransactionOutput>::T as Transaction>::Key,
        <<T as TransactionOutput>::T as Transaction>::Value,
    )> {
        let output = self.output.read().unwrap();
        let t = match output.as_ref().unwrap() {
            ExecutionStatus::Success(t) => t,
            ExecutionStatus::SkipRest(t) => t,
            ExecutionStatus::Abort(err) => return Vec::new(),
        };
        return t.get_writes();
    }

    pub fn output(&self) -> ExecutionStatus<T, Error<E>> {
        self.output.write().unwrap().take().unwrap()
    }
}

pub struct Scheduler<K, T, E> {
    // Shared index (version) of the next txn to be executed from the original transaction sequence.
    execution_marker: AtomicUsize,
    // Shared validation marker:
    // First 32 bits: next index to validate, resets (is decreased) upon aborts.
    // Last 32 bits: generation counter that's incremented every time the validation index is decreased.
    validation_marker: AtomicU64,
    // Stores number of threads that are currently validating. Used in combination with validation
    // marker to decide when it's safe to complete computation (can commit everything). TODO: a per-thread
    // counter would generalize to detect what prefix can be committed.
    num_validating: AtomicUsize,
    // Shared marker that's set when a thread detects all txns can be committed - so
    // other threads can immediately know without expensive checks.
    done_marker: AtomicUsize,

    // Shared number of txns to execute: updated before executing a block or when an error or
    // reconfiguration leads to early stopping (at that transaction version).
    stop_at_version: AtomicUsize,

    txn_buffer: SegQueue<usize>, // shared queue of list of dependency-resolved transactions.
    txn_dependency: Vec<Arc<Mutex<Vec<usize>>>>, // version -> txns that depend on it.
    txn_status: Vec<CachePadded<ArcSwapOption<STMStatus<K, T, E>>>>, // version -> execution status.

    // Separately for perf.
    exec_ids: Vec<CachePadded<AtomicUsize>>,
}

impl<K, T: TransactionOutput, E: Send + Clone> Scheduler<K, T, E> {
    pub fn new(num_txns: usize) -> Self {
        Self {
            execution_marker: AtomicUsize::new(0),
            validation_marker: AtomicU64::new(0),
            num_validating: AtomicUsize::new(0),
            done_marker: AtomicUsize::new(0),
            stop_at_version: AtomicUsize::new(num_txns),
            txn_buffer: SegQueue::new(),
            txn_dependency: (0..num_txns)
                .map(|_| Arc::new(Mutex::new(Vec::new())))
                .collect(),
            txn_status: (0..num_txns)
                .map(|_| CachePadded::new(ArcSwapOption::empty()))
                .collect(),
            exec_ids: (0..num_txns)
                .map(|_| CachePadded::new(AtomicUsize::new(0)))
                .collect(),
        }
    }

    pub fn decrease_validation_marker(&self, target_version: usize) {
        let mut val_marker = self.validation_marker.load(Ordering::SeqCst);
        loop {
            let val_version = val_marker >> 32;

            if (val_version as usize) < target_version {
                // Already below the desired index (read w. SeqCst), so no need to
                // decrease (target_version will be validated, and exec_id visible).
                // Note: can't abort when it's equal - a CAS in next_txn_to_validate
                // can be covering w. the same val_marker and increase back. '<' is
                // fine because a covering CAS can't already exist: must read later.
                return;
            }

            let new_num_decrease = (val_marker & MASK) + 1;
            let new_marker = (((target_version as u64) << 32) as u64) | new_num_decrease;

            match self.validation_marker.compare_exchange_weak(
                val_marker,
                new_marker,
                Ordering::SeqCst, // Make sure my decrease is sequentially consistent.
                Ordering::SeqCst, // Re-read w. seqcst semantics
            ) {
                Ok(_) => {
                    // Successfully updated.
                    return;
                }
                Err(x) => {
                    val_marker = x;
                }
            }
        }
    }

    pub fn abort(&self, version: usize, cur_status: &Arc<STMStatus<K, T, E>>) -> bool {
        if let Ok(_) = self.exec_ids[version].compare_exchange(
            cur_status.exec_id(),
            cur_status.exec_id() + 1,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            // Safe to decrease here because the caller marks dirty, then re-executes.
            self.decrease_validation_marker(version);

            // Successfully aborted.
            return true;
        }
        false
    }

    // Return the next txn version & status for the thread to validate.
    pub fn next_txn_to_validate(&self) -> Option<(usize, Arc<STMStatus<K, T, E>>)> {
        // Read val_marker, seq cst ordered for status reads (here below).
        let mut val_marker = self.validation_marker.load(Ordering::SeqCst);

        loop {
            let next_to_val = (val_marker >> 32) as usize;
            if next_to_val >= self.num_txn_to_execute() {
                return None;
            }

            // Read a seq consistent exec_id watermark and only validate a status with
            // a matching exec_id.
            let exec_id_watermark = self.exec_id(next_to_val);
            if (exec_id_watermark & 1) == 0 {
                // Even watermark means awating (re-)execution, not ready to validate.
                return None;
            }

            let num_decrease = val_marker & MASK;
            let new_marker = ((next_to_val as u64 + 1) << 32) | num_decrease;

            // Mark that thread is validating next_to_val, else early abort possible.
            self.num_validating.fetch_add(1, Ordering::SeqCst);

            // CAS to win the competition to actually validate next_to_val.
            match self.validation_marker.compare_exchange_weak(
                val_marker,
                new_marker,
                Ordering::SeqCst, // Keep status stores above.
                Ordering::SeqCst, // Re-read w. seqcst semantics
            ) {
                Ok(_) => {
                    // Could assert that status retry number isn't lower than watermark

                    // CAS successful, return index & status.
                    return Some((
                        next_to_val,
                        self.txn_status[next_to_val].load_full().unwrap().clone(),
                    ));
                }
                Err(x) => {
                    // CAS unsuccessful - not validating next_to_val (will try different index).
                    if self.finish_validation() {
                        self.check_done();
                    }
                    val_marker = x;
                }
            }
        }
    }

    // Return the next txn id for the thread to execute: first fetch from the shared queue that
    // stores dependency-resolved txns, then fetch from the original ordered txn sequence.
    // Return Some(id) if found the next transaction, else return None.
    pub fn next_txn_to_execute(&self) -> Option<Version> {
        // Fetch txn from txn_buffer
        match self.txn_buffer.pop() {
            Some(version) => Some(version),
            None => {
                // Fetch the first non-executed txn from the original transaction list
                let next_to_execute = self.execution_marker.fetch_add(1, Ordering::Relaxed);
                if next_to_execute < self.num_txn_to_execute() {
                    Some(next_to_execute)
                } else {
                    // Everything executed at least once - validation will take care of rest.
                    None
                }
            }
        }
    }

    // Invoked when txn depends on another txn, adds version to the dependency list the other txn.
    // Return true if successful, otherwise dependency resolved in the meantime, return false.
    pub fn add_dependency(&self, version: Version, dep_version: Version) -> bool {
        // Could pre-check that the txn isn't in executed state, but shouldn't matter much since
        // the caller usually has just observed the read dependency (so not executed state).

        // txn_dependency is initialized for all versions, so unwrap() is safe.
        let mut stored_deps = self.txn_dependency[dep_version].lock().unwrap();
        if self.exec_id(dep_version) & 1 == 0 {
            stored_deps.push(version);
            return true;
        }
        false
    }

    pub fn exec_id(&self, version: usize) -> usize {
        self.exec_ids[version].load(Ordering::SeqCst)
    }

    pub fn output(&self, version: usize) -> ExecutionStatus<T, Error<E>> {
        // Executed after scheduler done to grab outputs, fine to load.
        self.txn_status[version].load().as_ref().unwrap().output()
    }

    // After txn is executed, add its dependencies to the shared buffer for execution.
    pub fn finish_execution(
        &self,
        version: usize,
        exec_id: usize,
        input: Vec<ReadDescriptor<K>>,
        output: ExecutionStatus<T, Error<E>>,
    ) {
        // Stores are safe because there is at most one execution at a time.
        self.txn_status[version].store(Some(Arc::new(STMStatus {
            exec_id: exec_id + 1,
            input,
            output: RwLock::new(Some(output)),
        })));
        self.exec_ids[version].store(exec_id + 1, Ordering::SeqCst);

        // we want to make things fast inside the lock, so use replace instead of clone
        let mut version_deps: Vec<usize> = {
            // we want to make things fast inside the lock, so use take instead of clone
            let mut stored_deps = self.txn_dependency[version].lock().unwrap();
            std::mem::take(&mut stored_deps)
        };

        version_deps.sort_unstable();
        for dep in version_deps {
            self.txn_buffer.push(dep);
        }
    }

    // Returns true if there are no active validators left.
    pub fn finish_validation(&self) -> bool {
        self.num_validating.fetch_sub(1, Ordering::SeqCst) == 1
    }

    // Reset the txn version/id to end execution earlier. The executor will stop at the smallest
    // `stop_version` when there are multiple concurrent invocation.
    pub fn set_stop_version(&self, stop_version: Version) {
        self.stop_at_version
            .fetch_min(stop_version, Ordering::Relaxed);
    }

    // Get the last txn version/id
    pub fn num_txn_to_execute(&self) -> Version {
        self.stop_at_version.load(Ordering::Relaxed)
    }

    // A lazy, relatively expensive check of whether the scheduler execution is completed.
    // Updates the 'done_marker' so other threads can know by calling done() function below.
    // Checks validation marker, takes min of validation index (val marker's first 32 bits)
    // and thread commit markers. If >= stop_version, re-reads val marker to ensure it
    // (in particular, gen counter) hasn't changed - otherwise a race is possible.
    pub fn check_done(&self) {
        let val_marker = self.validation_marker.load(Ordering::SeqCst);

        let num_txns = self.num_txn_to_execute();
        let val_version = (val_marker >> 32) as usize;
        if val_version < num_txns || self.num_validating.load(Ordering::SeqCst) > 0 {
            // There are txns to validate.
            return;
        }

        // Re-read and make sure it hasn't changed. If so, everything can be committed
        // and set the done flag.
        if val_marker == self.validation_marker.load(Ordering::SeqCst) {
            self.done_marker.store(1, Ordering::Release);
        }
    }

    // Checks whether the done marker is set. The marker can only be set by 'check_done'.
    pub fn done(&self) -> bool {
        self.done_marker.load(Ordering::Acquire) == 1
    }
}
