use arc_swap::ArcSwap;
use crossbeam::utils::CachePadded;
use crossbeam_queue::SegQueue;
use diem_types::transaction::TransactionOutput;
use diem_types::{access_path::AccessPath, write_set::WriteSet};
use move_core_types::vm_status::VMStatus;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, RwLock,
};

#[derive(Clone)]
pub struct ReadDescriptor {
    access_path: AccessPath,

    // Is set to (version, retry_num) if the read was from shared MV data-structure
    // (written by a previous txn), None if read from storage.
    version_and_retry_num: Option<(usize, usize)>, // TODO: Version alias for usize.
}

impl ReadDescriptor {
    pub fn new(access_path: AccessPath, version_and_retry_num: Option<(usize, usize)>) -> Self {
        Self {
            access_path,
            version_and_retry_num,
        }
    }

    pub fn path(&self) -> &AccessPath {
        &self.access_path
    }

    pub fn validate(&self, new_version_and_retry_num: Option<(usize, usize)>) -> bool {
        self.version_and_retry_num == new_version_and_retry_num
    }
}

// Transaction status, updated using RCU. Starts with retry_num = 0 and input_output = None
// that signifies NOT_EXECUTED status. When the transaction is first executed, new STMStatus
// w. retry_num = 1 and the input/output of the corresponding execution will be stored.
// If the transaction is later aborted, the status will become (1, None).
pub struct STMStatus {
    retry_num: usize, // how many times the corresponding txn has been re-tried.

    // Transactions input/output set from the last execution (for all actual reads and writes).
    // For reads we just store (version, retry_num) pairs. TODO: Version alias for usize.
    // If None, then the corresponding retry_num execution hasn't yet completed.
    input_output: Option<(Vec<ReadDescriptor>, (VMStatus, TransactionOutput))>,
}

impl STMStatus {
    pub fn new_before_execution(retry_num: usize) -> Self {
        Self {
            retry_num,
            input_output: None,
        }
    }

    pub fn new_after_execution(
        retry_num: usize,
        input: Vec<ReadDescriptor>,
        output: (VMStatus, TransactionOutput),
    ) -> Self {
        Self {
            retry_num,
            input_output: Some((input, output)),
        }
    }

    pub fn executed(&self) -> bool {
        self.input_output.is_some()
    }

    pub fn retry_num(&self) -> usize {
        self.retry_num
    }

    // executed() must hold, i.e. input_output != None. Returns a reference to the inner Vec.
    pub fn read_set(&self) -> &Vec<ReadDescriptor> {
        &self.input_output.as_ref().unwrap().0
    }

    // executed() must hold, i.e. input_output != None. Returns a reference to the inner writeset.
    pub fn write_set(&self) -> &WriteSet {
        let output = &self.input_output.as_ref().unwrap().1;
        &output.1.write_set()
    }

    pub fn output(&self) -> (VMStatus, TransactionOutput) {
        self.input_output.as_ref().unwrap().1.clone()
    }
}

pub struct Scheduler {
    // Shared index (version) of the next txn to be executed from the original transaction sequence.
    execution_marker: AtomicUsize,
    // Shared validation marker:
    // First 32 bits: next index to validate, resets (is decreased) upon aborts.
    // Last 32 bits: generation counter that's incremented every time the validation index is decreased.
    validation_marker: AtomicU64,
    // Stores per thread transaction version it is validating, or max::usize if the thread isn't
    // validating. Min of these values and validation index (if read atomically) is safe to commit.
    thread_commit_markers: Vec<CachePadded<AtomicUsize>>,
    // Shared marker that's set when a thread detects all txns can be committed - so
    // other threads can immediately know without expensive checks.
    done_marker: AtomicUsize,

    // Shared number of txns to execute: updated before executing a block or when an error or
    // reconfiguration leads to early stopping (at that transaction version).
    stop_at_version: AtomicUsize,

    txn_buffer: SegQueue<usize>, // shared queue of list of dependency-resolved transactions.
    txn_dependency: Vec<CachePadded<Arc<RwLock<Option<Vec<usize>>>>>>, // version -> txns that depend on it.
    txn_status: Vec<CachePadded<ArcSwap<STMStatus>>>, // version -> execution status.
}

impl Scheduler {
    pub fn new(num_txns: usize, num_threads: usize) -> Self {
        Self {
            execution_marker: AtomicUsize::new(0),
            validation_marker: AtomicU64::new(0),
            thread_commit_markers: (0..num_threads)
                .map(|_| CachePadded::new(AtomicUsize::new(usize::MAX)))
                .collect(),
            done_marker: AtomicUsize::new(0),
            stop_at_version: AtomicUsize::new(num_txns),
            txn_buffer: SegQueue::new(),
            txn_dependency: (0..num_txns)
                .map(|_| CachePadded::new(Arc::new(RwLock::new(Some(Vec::new())))))
                .collect(),
            txn_status: (0..num_txns)
                .map(|_| {
                    CachePadded::new(ArcSwap::from_pointee(STMStatus::new_before_execution(0)))
                })
                .collect(),
        }
    }

    fn decrease_validation_marker(&self, target_version: usize) {
        loop {
            let val_marker = self.validation_marker.load(Ordering::Acquire);

            let val_version = val_marker >> 32;
            if val_version as usize <= target_version {
                // Already below the desired index, no need to decrease (idx will be validated).
                return;
            }

            let new_num_decrease = (val_marker & (1 << 32 - 1)) + 1;
            // TODO: assert no overflow.
            let new_marker = ((target_version << 32) as u64) | new_num_decrease;

            if let Ok(_) = self.validation_marker.compare_exchange(
                val_marker,
                new_marker,
                Ordering::Release, // Keep stores above.
                Ordering::Relaxed, // Just read latest marker.
            ) {
                // Successfully updated.
                return;
            }
        }
    }

    pub fn abort(&self, version: usize, cur_status: &Arc<STMStatus>) -> bool {
        let stored_ptr = self.txn_status[version].compare_and_swap(
            cur_status,
            Arc::new(STMStatus::new_before_execution(cur_status.retry_num() + 1)),
        );

        if Arc::ptr_eq(&stored_ptr, cur_status) {
            // The corresponding re-execution was already aborted, nothing to do.
            return false;
        }

        // Successfully aborted.
        self.decrease_validation_marker(version);
        true
    }

    // Return the next txn version & status for the thread to validate.
    pub fn next_txn_to_validate(&self, thread_id: usize) -> Option<(usize, Arc<STMStatus>)> {
        loop {
            let val_marker = self.validation_marker.load(Ordering::Acquire); // status read below.

            let next_to_val = (val_marker >> 32) as usize;
            if next_to_val >= self.num_txn_to_execute() {
                return None;
            }
            let next_status = self.txn_status[next_to_val].load_full();
            if !next_status.executed() {
                // No more transactions or next txn not yet (re-)executed to validate.
                return None;
            }

            let num_decrease = val_marker & (1 << 32 - 1);
            let new_marker = ((next_to_val as u64 + 1) << 32) | num_decrease;
            if let Ok(_) = self.validation_marker.compare_exchange(
                val_marker,
                new_marker,
                Ordering::Release, // Keep stores above.
                Ordering::Relaxed, // Just read latest marker.
            ) {
                // CAS successful, mark that thread is validating, return index & status.
                self.thread_commit_markers[thread_id].store(next_to_val, Ordering::Release);
                return Some((next_to_val, next_status));
            }
        }
    }

    // Return the next txn id for the thread to execute: first fetch from the shared queue that
    // stores dependency-resolved txns, then fetch from the original ordered txn sequence.
    // Return Some(id) if found the next transaction, else return None.
    pub fn next_txn_to_execute(&self) -> Option<usize> {
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
    // Return Some(true) if successful, otherwise dependency resolved in the meantime, return None.
    pub fn add_dependency(&self, version: usize, dep_version: usize) -> bool {
        // Could pre-check that the txn isn't in executed state, but shouldn't matter much since
        // the caller usually has just observed the read dependency (so not executed state).

        // txn_dependency is initialized for all versions, so unwrap() is safe.
        let mut stored_deps = self.txn_dependency[version].write().unwrap();
        if !self.txn_status[dep_version].load().executed() {
            stored_deps.as_mut().unwrap().push(version);
            return true;
        }
        false
    }

    pub fn retry_num(&self, version: usize) -> usize {
        self.txn_status[version].load().retry_num()
    }

    pub fn output(&self, version: usize) -> (VMStatus, TransactionOutput) {
        self.txn_status[version].load().output()
    }

    // After txn is executed, add its dependencies to the shared buffer for execution.
    pub fn finish_execution(
        &self,
        version: usize,
        retry_num: usize,
        input: Vec<ReadDescriptor>,
        output: (VMStatus, TransactionOutput),
    ) {
        let mut version_deps = Some(Vec::new());
        // Store is safe because of an invariant that there is at most one execution at a time.
        self.txn_status[version].store(Arc::new(STMStatus::new_after_execution(
            retry_num, input, output,
        )));

        {
            // we want to make things fast inside the lock, so use replace instead of clone
            let mut stored_deps = self.txn_dependency[version].write().unwrap();
            if !stored_deps.as_mut().unwrap().is_empty() {
                version_deps = stored_deps.replace(Vec::new());
            }
        }

        let deps = version_deps.as_mut().unwrap();
        deps.sort();
        for dep in deps {
            self.txn_buffer.push(*dep);
        }
    }

    pub fn finish_validation(&self, thread_id: usize) {
        self.thread_commit_markers[thread_id].store(usize::MAX, Ordering::Release);
    }

    // Reset the txn version/id to end execution earlier
    pub fn set_stop_version(&self, stop_version: usize) {
        self.stop_at_version
            .fetch_min(stop_version, Ordering::Relaxed);
    }

    // Get the last txn version/id
    pub fn num_txn_to_execute(&self) -> usize {
        return self.stop_at_version.load(Ordering::Relaxed);
    }

    // A lazy, relatively expensive check of whether the scheduler execution is completed.
    // Updates the 'done_marker' so other threads can know by calling done() function below.
    // Checks validation marker, takes min of validation index (val marker's first 32 bits)
    // and thread commit markers. If >= stop_version, re-reads val marker to ensure it
    // (in particular, gen counter) hasn't changed - otherwise a race is possible.
    pub fn check_done(&self) {
        let val_marker = self.validation_marker.load(Ordering::Acquire);

        let num_txns = self.num_txn_to_execute();
        let val_version = (val_marker >> 32) as usize;
        if val_version < num_txns
            || self
                .thread_commit_markers
                .iter()
                .any(|marker| marker.load(Ordering::Acquire) < num_txns)
        {
            // There are txns to validate.
            return;
        }

        // Re-read and make sure it hasn't changed. If so, everything can be committed
        // and set the done flag.
        if val_marker == self.validation_marker.load(Ordering::Acquire) {
            self.done_marker.store(1, Ordering::Release);
        }
    }

    // Checks whether the done marker is set. The marker can only be set by 'check_done'.
    pub fn done(&self) -> bool {
        self.done_marker.load(Ordering::Acquire) == 1
    }

    // TODO: Implement for debugging & measurements.
    // pub fn sum_retry_num(&self) -> usize {
    //     let num = self.stop_when.load(Ordering::SeqCst);
    //     let mut sum = 0;
    //     for i in 0..num {
    //         sum += self.retry_num_vec[i].load(Ordering::SeqCst);
    //     }
    //     return sum;
    // }

    // pub fn print_info(&self) {
    //     let val = self.val_index.lock().unwrap().load(Ordering::SeqCst);
    //     let commit_idx = self.commit_index.load(Ordering::SeqCst);
    //     let execute = self.execute_idx.load(Ordering::SeqCst);
    //     println!(
    //         "Thread {:?} commit_idx {} val_idx {} execute_idx {}",
    //         thread::current().id(),
    //         commit_idx,
    //         val,
    //         execute
    //     );
    // }
}
