use crossbeam_queue::SegQueue;
use std::{
    thread,
    cmp::{max, min},
    sync::{
        atomic::{AtomicUsize, AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    convert::{TryFrom,TryInto},
};
use diem_types::transaction::TransactionOutput;
use move_core_types::vm_status::VMStatus;

const FLAG_EXECUTED: u64 = 0;
const FLAG_VALIDATING: u64 = 1;
const FLAG_NOT_EXECUTED: u64 = 2;
const FLAG_VALIDATED: u64 = 3;

// Data structure (shared among all threads):
// signature_verified_block[curent_idx]: original transactions in order
// deps_mapping: hashmap that maps a txn_id to a set of transactions, used for tracking the set of transactions that are scheduled to re-execute once the txn of txn_id is executed
// txn_id_buffer: list of transactions that are dependency-resolved (optimistically) and scheduled to re-execute


#[cfg_attr(any(target_arch = "x86_64"), repr(align(128)))]
pub struct DepStruct {
    dep_vec: Arc<RwLock<Option<Vec<usize>>>>, // set of txn_ids that depends on the txn, set to Option for replacing the vec inside the lock efficiently
}

impl DepStruct {
    pub fn new() -> Self {
        Self {
            dep_vec: Arc::new(RwLock::new(Some(Vec::new()))),
        }
    }
}

pub struct Scheduler {
    execute_idx: AtomicUsize,        // the shared txn index of the next txn to be executed from the original transaction list
    val_index: Arc<Mutex<AtomicUsize>>,         // the shared txn index of the next txn to be validated
    commit_index: AtomicUsize,         // the shared txn index of the next txn to be committed
    deps_mapping: Vec<DepStruct>, // the shared mapping from a txn to txns that are dependent on the txn
    txn_id_buffer: SegQueue<usize>, // the shared queue of list of transactions that are dependency-resolved
    stop_when: AtomicUsize,       // recording number of txns
    status_vec: Vec<AtomicU64>,     // each txn's status, including not executed, executed, (reval_num, validated), (reval_num, validating), reval_num is 62bits
    val_index_back_num: AtomicU64,    // the number of times that val_index goes back, initially 0,
    retry_num_vec: Vec<AtomicUsize>,    // the number of re-executions of each txn, initially 0
    last_execution_reads_version_vec: Vec<Arc<RwLock<Vec<(bool, Option<(usize, usize)>)>>>>,   // (version, retry_num) vec of each txn's last reads from the last execution, flag=true if no read-dependency
    last_execution_output: Vec<Arc<RwLock<Option<(VMStatus, TransactionOutput)>>>>,   // vector of each txn's output from the last execution
}

impl Scheduler {
    pub fn new(num_txns: usize) -> Self {
        Self {
            execute_idx: AtomicUsize::new(0),
            val_index: Arc::new(Mutex::new(AtomicUsize::new(0))),
            commit_index: AtomicUsize::new(0),
            deps_mapping: (0..num_txns).map(|_| DepStruct::new()).collect(),
            txn_id_buffer: SegQueue::new(),
            stop_when: AtomicUsize::new(num_txns),
            status_vec: (0..num_txns).map(|_| AtomicU64::new(FLAG_NOT_EXECUTED)).collect(),
            val_index_back_num: AtomicU64::new(1),
            retry_num_vec: (0..num_txns).map(|_| AtomicUsize::new(0)).collect(),
            last_execution_reads_version_vec: (0..num_txns).map(|_| Arc::new(RwLock::new(Vec::new()))).collect(),
            last_execution_output: (0..num_txns).map(|_| Arc::new(RwLock::new(None))).collect(),
        }
    }

    fn get_status_bits(&self, idx: usize) -> u64 {
        let status_bits = self.status_vec[idx].load(Ordering::SeqCst);
        return status_bits & 3; // AND with bits 11 to get the status bits
    }

    fn get_val_index_back_num_bits(&self, idx: usize) -> u64 {
        let status_bits = self.status_vec[idx].load(Ordering::SeqCst);
        return status_bits >> 2; // Right shift 2 bits to get the retry numbers
    }

    pub fn abort(&self, idx: usize, back_num: u64) -> (bool, u64) {
        let val = self.val_index.lock().unwrap();
        let not_executed_bits = (back_num << 2) | FLAG_NOT_EXECUTED;
        let pre = self.status_vec[idx].fetch_max(not_executed_bits, Ordering::SeqCst);
        if pre < not_executed_bits {
            let new_back_num = self.increment_val_index_back_num();
            let next_to_val = val.load(Ordering::SeqCst);
            val.store(min(next_to_val, idx), Ordering::SeqCst);
            return (true, new_back_num);
        }
        return (false, back_num);
    }

    pub fn set_executed(&self, idx: usize, back_num: u64) {
        let executed_bits = (back_num << 2) | FLAG_EXECUTED;
        let pre = self.status_vec[idx].fetch_max(executed_bits, Ordering::SeqCst);
        assert_eq!((pre >> 2) < back_num, true);
    }

    pub fn set_validated(&self, idx: usize, back_num: u64) -> bool {
        let validated_bits = (back_num << 2) | FLAG_VALIDATED;
        let pre = self.status_vec[idx].fetch_max(validated_bits, Ordering::SeqCst);
        return pre < validated_bits;
    }

    pub fn get_retry_num(&self, idx: usize) -> usize {
        return self.retry_num_vec[idx].load(Ordering::SeqCst);
    }

    pub fn increment_retry_num(&self, idx: usize) -> usize {
        self.retry_num_vec[idx].fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn get_val_index_back_num(&self) -> u64 {
        return self.val_index_back_num.load(Ordering::SeqCst);
    }

    pub fn increment_val_index_back_num(&self) -> u64 {
        self.val_index_back_num.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn sum_retry_num(&self) -> usize {
        let num = self.stop_when.load(Ordering::SeqCst);
        let mut sum = 0;
        for i in 0..num {
            sum += self.retry_num_vec[i].load(Ordering::SeqCst);
        }
        return sum;
    }

    // Return the next txn id and revalication num for the thread to validate
    pub fn next_txn_to_validate(&self) -> Option<(usize, u64)> {
        let val = self.val_index.lock().unwrap();
        let next_to_val = val.load(Ordering::SeqCst);
        if next_to_val >= self.get_txn_num() || self.get_status_bits(next_to_val) == FLAG_NOT_EXECUTED  {
            return None;
        }
        let back_num = self.get_val_index_back_num();
        let validating_bits = (back_num << 2) | FLAG_VALIDATING;
        self.status_vec[next_to_val].store(validating_bits, Ordering::SeqCst);
        val.store(next_to_val+1, Ordering::SeqCst);
        return Some((next_to_val, back_num));
    }

    // Return the next txn id for the thread to execute,
    // first fetch from the shared queue that stores dependency-resolved txn_ids,
    // then fetch from the original txn list.
    // Return Some(id) if found the next transaction, else return None.
    pub fn next_txn_to_execute(&self) -> Option<usize> {
        match self.txn_id_buffer.pop() {
            // Fetch txn from txn_id_buffer
            Some(idx) => return Some(idx),
            None => {
                loop {
                    // Fetch the first non-executed txn from the orginal transaction list
                    let next_to_execute = self.execute_idx.load(Ordering::SeqCst);
                    if next_to_execute >= self.get_txn_num() {
                        return None;
                    }
                    let status = self.get_status_bits(next_to_execute);
                    if status == FLAG_NOT_EXECUTED {
                        match self.execute_idx.compare_exchange(next_to_execute, next_to_execute+1, Ordering::SeqCst, Ordering::SeqCst) {
                            Ok(_) => return Some(next_to_execute),
                            Err(_) => continue,
                        }
                    } else {
                        match self.execute_idx.compare_exchange(next_to_execute, next_to_execute+1, Ordering::SeqCst, Ordering::SeqCst) {
                            _ => continue,
                        }
                    }
                }
            }
        };
    }

    // Invoked when txn idx depends on txn dep_id,
    // will add txn idx to the dependency list of txn dep_id.
    // Return Some(true) if successful, otherwise txn dep_id are executed, return None.
    pub fn update_read_deps(&self, idx: usize, dep_id: usize) -> Option<bool> {
        let status = self.get_status_bits(dep_id);
        if status == FLAG_EXECUTED {
            return None;
        }
        // The data structures of dep_vec are initialized for all txn_ids, so unwrap() is safe.
        let mut deps = self.deps_mapping[dep_id].dep_vec.write().unwrap();
        let status = self.get_status_bits(dep_id);
        if status == FLAG_NOT_EXECUTED {
            deps.as_mut().unwrap().push(idx);
            return Some(true);
        }
        return None;
    }

    // After txn idx is executed, add the list of txns that depends on txn idx to the shared queue.
    pub fn update_dep_after_execution(&self, idx: usize) {
        let mut txn_set = Some(Vec::new());
        {
            // we want to make things fast inside the lock, so use replace instead of clone
            let mut deps = self.deps_mapping[idx].dep_vec.write().unwrap();
            if !deps.as_mut().unwrap().is_empty() {
                txn_set = deps.replace(Vec::new());
            }
        }
        let txns = txn_set.as_mut().unwrap();
        txns.sort();
        for ids in txns {
            self.txn_id_buffer.push(*ids);
        }
    }

    // Reset the txn version/id to end execution earlier
    pub fn set_stop_version(&self, num: usize) {
        self.stop_when.fetch_min(num, Ordering::SeqCst);
    }

    // Get the last txn version/id
    pub fn get_txn_num(&self) -> usize {
        return self.stop_when.load(Ordering::SeqCst);
    }

    pub fn update_last_reads(&self, idx: usize, vec: Vec<(bool, Option<(usize, usize)>)>) {
        let mut last_reads = self.last_execution_reads_version_vec[idx].write().unwrap();
        *last_reads = vec;
    }

    pub fn get_last_reads(&self, idx: usize) -> Vec<(bool, Option<(usize, usize)>)> {
        return (*self.last_execution_reads_version_vec[idx].read().unwrap()).clone();
    }

    pub fn update_last_output(&self, idx: usize, output: (VMStatus, TransactionOutput)) {
        let mut last_output = self.last_execution_output[idx].write().unwrap();
        *last_output = Some(output);
    }

    pub fn get_last_output(&self, idx: usize) -> Option<(VMStatus, TransactionOutput)> {
        return (*self.last_execution_output[idx].read().unwrap()).clone();
    }

    // Validation checks if the version vector during validation matches with the reads of the previous execution
    pub fn validation(&self, idx: usize, validation_reads_version_vec: Vec<(bool, Option<(usize, usize)>)>) -> bool {
        let last_reads = self.get_last_reads(idx);
        return validation_reads_version_vec == last_reads;
    }

    // Commit the prefix of txns that are validated
    pub fn commit(&self) {
        loop {
            let next_to_commit = self.commit_index.load(Ordering::SeqCst);
            if next_to_commit >= self.get_txn_num() || next_to_commit >= self.val_index.lock().unwrap().load(Ordering::SeqCst) {
                return;
            }
            let status = self.get_status_bits(next_to_commit);
            if status == FLAG_VALIDATED {
                match self.commit_index.compare_exchange(next_to_commit, next_to_commit+1, Ordering::SeqCst, Ordering::SeqCst) {
                    _ => continue,
                }
            } else {
                return;
            }
        }
    }

    pub fn all_committed(&self) -> bool {
        return self.commit_index.load(Ordering::SeqCst) >= self.get_txn_num();
    }

    pub fn print_info(&self) {
        let val = self.val_index.lock().unwrap().load(Ordering::SeqCst);
        let commit_idx = self.commit_index.load(Ordering::SeqCst);
        let execute = self.execute_idx.load(Ordering::SeqCst);
        println!("Thread {:?} commit_idx {} val_idx {} execute_idx {}",
        thread::current().id(), commit_idx, val, execute);
    }
}
