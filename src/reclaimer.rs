use std::{thread_local, cell::RefCell, sync::atomic::AtomicUsize, collections::HashSet};

use crate::block_bag::{BlockBag, Block};

thread_local! {
    pub static TEMP_PATIENCE: RefCell<usize> = RefCell::new(0);
}

const OPS_BEFORE_NEUTRALIZE: usize = 1000;

struct Thread {
    // Retired records collected by a delete operation
    retired: Vec<BlockBag>,
    // Saves the discovered records before upgrading to write
    // to protect records from concurrent reclaimer threads.
    proposed_hazptrs: Vec<AtomicUsize>,
    // Each reclaimer scans hazard pointers across threads
    // to free retired its bag such that any hazard pointers aren't freed.
    scanned_hazptrs: Vec<HashSet<usize>>,

    // Used for NBR+ signal optimization
    announced_ts: AtomicUsize,
    saved_ts: usize,
    saved_block_head: Option<Block>,
}

struct ReclaimerNBR {
    num_process: usize,
    threads: Vec<Thread>,
}

impl ReclaimerNBR {
    pub fn new(num_process: usize, ) -> Self {

    }
}