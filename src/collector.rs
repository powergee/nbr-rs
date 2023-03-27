/// A concurrent garbage collector
/// with Neutralization Based Reclamation (NBR+).
use nix::sys::pthread::pthread_self;
use std::sync::atomic::AtomicU64;
use std::sync::Barrier;
use std::{
    cell::RefCell,
    collections::HashSet,
    ptr::null_mut,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

use crate::block_bag::{Block, BlockBag, BlockPool, BLOCK_SIZE};
use crate::recovery;

const OPS_BEFORE_TRYRECLAIM_LOWATERMARK: usize = 16000;
const MAX_RINGBAG_CAPACITY_POW2: usize = 32768;

/// 0-indexed thread identifier.
/// Note that this ThreadId is not same with pthread_t,
/// and it is used for only NBR internally.
pub type ThreadId = usize;

/// Thread-local variables of NBR+
struct Thread {
    tid: ThreadId,
    // Retired records collected by a delete operation
    retired: BlockBag,
    // Saves the discovered records before upgrading to write
    // to protect records from concurrent reclaimer threads.
    // (Single-Writer Multi-Reader)
    proposed_len: AtomicUsize,
    proposed_hazptrs: Vec<AtomicPtr<u8>>,
    // Each reclaimer scans hazard pointers across threads
    // to free retired its bag such that any hazard pointers aren't freed.
    scanned_hazptrs: RefCell<HashSet<*mut u8>>,
    // A helper to allocate and recycle blocks
    //
    // In the original implementation, `reclaimer_nbr` has
    // `pool` as a member, which is a subclass of `pool_interface`.
    // And `pool_interface` has `blockpools` to serve a thread-local
    // block pool to each worker.
    // We don't have to write any codes which is equivalent to
    // `pool_interface`, as `pool_interface` is just a simple
    // wrapper for convinient allocating & deallocating.
    // It can be replaced with a simple `BlockPool`.
    //
    // Also note that `retired` is declared before `pool`,
    // and it makes sure that `retired` is dropped before
    // dropping `pool`.
    pool: *mut BlockPool,
    temp_patience: usize,

    // Used for NBR+ signal optimization
    announced_ts: AtomicUsize,
    saved_ts: Vec<usize>,
    saved_block_head: *mut Block,
    first_lo_entry_flag: bool,
    retires_since_lo_watermark: usize,
}

impl Thread {
    // This out of patience decision has been improved
    // in the latest NBR+ version on setbench.
    #[inline]
    fn is_out_of_patience(&mut self) -> bool {
        if self.temp_patience == 0 {
            self.temp_patience = MAX_RINGBAG_CAPACITY_POW2 / BLOCK_SIZE;
        }

        self.retired.size_in_blocks() > self.temp_patience
    }

    #[inline]
    fn is_past_lo_watermark(&mut self) -> bool {
        (self.retired.size_in_blocks() as f32
            > (self.temp_patience as f32 * (1.0f32 / ((self.tid % 3) + 2) as f32)))
            && ((self.retires_since_lo_watermark % OPS_BEFORE_TRYRECLAIM_LOWATERMARK) == 0)
    }

    #[inline]
    fn set_lo_watermark(&mut self) {
        self.saved_block_head = self.retired.first_non_empty_block();
    }

    #[inline]
    fn send_freeable_to_pool(&mut self) {
        if !self.saved_block_head.is_null() {
            // Reclaim due to a lo-watermark path
            let mut spare_bag = BlockBag::new(self.pool);

            while !self.retired.is_empty()
                && self.retired.first_non_empty_block() != self.saved_block_head
            {
                let ret = self.retired.remove();
                spare_bag.add_retired(ret);
            }

            // Deallocate blocks and clear the bag.
            // It may not reclaim non full blocks.
            while !self.retired.is_empty() {
                let ret = self.retired.remove();
                unsafe { ret.deallocate() };
            }

            while !spare_bag.is_empty() {
                self.retired.add_retired(spare_bag.remove());
            }
        } else {
            // Reclaim due to a hi-watermark path
            let mut spare_bag = BlockBag::new(self.pool);

            while !self.retired.is_empty() {
                let ret = self.retired.remove();
                if self.scanned_hazptrs.borrow().contains(&ret.ptr()) {
                    spare_bag.add_retired(ret);
                } else {
                    unsafe { ret.deallocate() };
                }
            }

            // Add all collected but protected records back to `retired`
            while !spare_bag.is_empty() {
                let ret = spare_bag.remove();
                self.retired.add_retired(ret);
            }
        }
    }
}

pub struct Collector {
    num_threads: usize,
    threads: Vec<Thread>,
    // Map from Thread ID into pthread_t(u64)
    // for each registered thread
    registered_map: Vec<AtomicU64>,
    registered_count: AtomicUsize,
    barrier: Barrier,
}

impl Collector {
    pub fn new(num_threads: usize, max_hazptr_per_thred: usize) -> Self {
        unsafe { recovery::install() };

        let threads = (0..num_threads)
            .map(|tid| {
                let pool = Box::<BlockPool>::default();
                let pool = Box::into_raw(pool);
                let retired = BlockBag::new(pool);
                let proposed_hazptrs = (0..max_hazptr_per_thred)
                    .map(|_| AtomicPtr::new(null_mut()))
                    .collect();
                let scanned_hazptrs = RefCell::default();
                let announced_ts = AtomicUsize::new(0);
                let saved_ts = vec![0; num_threads];
                let temp_patience = MAX_RINGBAG_CAPACITY_POW2 / BLOCK_SIZE;

                Thread {
                    tid,
                    retired,
                    proposed_len: AtomicUsize::new(0),
                    proposed_hazptrs,
                    scanned_hazptrs,
                    pool,
                    temp_patience,
                    announced_ts,
                    saved_ts,
                    saved_block_head: null_mut(),
                    first_lo_entry_flag: true,
                    retires_since_lo_watermark: 0,
                }
            })
            .collect();

        Self {
            num_threads,
            threads,
            registered_map: (0..num_threads).map(|_| AtomicU64::new(0)).collect(),
            registered_count: AtomicUsize::new(0),
            barrier: Barrier::new(num_threads),
        }
    }

    #[inline]
    unsafe fn restart_all_threads(&self, reclaimer: ThreadId) {
        for other_tid in 0..self.num_threads {
            if other_tid == reclaimer {
                continue;
            }
            let pthread = self.registered_map[other_tid].load(Ordering::Acquire);
            if let Err(err) = recovery::send_signal(pthread) {
                panic!("Failed to restart other threads: {err}");
            }
        }
    }

    #[inline]
    fn collect_all_saved_records(&self, reclaimer: ThreadId) {
        // Set where record would be collected in.
        let mut scanned = self.threads[reclaimer].scanned_hazptrs.borrow_mut();
        scanned.clear();

        for other_tid in 0..self.num_threads {
            let len = self.threads[other_tid].proposed_len.load(Ordering::Acquire);
            for i in 0..len {
                let hazptr = &self.threads[other_tid].proposed_hazptrs[i];
                let ptr = hazptr.load(Ordering::Acquire);
                scanned.insert(ptr);
            }
        }
    }

    #[inline]
    fn reclaim_freeable(&mut self, reclaimer: ThreadId) {
        self.collect_all_saved_records(reclaimer);
        self.threads[reclaimer].send_freeable_to_pool();
    }

    pub fn register(&self) -> Guard {
        // Initialize current thread.
        // (ref: `initThread(tid)` in `recovery_manager.h`
        //  from original nbr_setbench)
        let tid = self.registered_count.fetch_add(1, Ordering::SeqCst);
        assert!(
            tid < self.num_threads,
            "Attempted to exceed the maximum number of threads"
        );
        self.registered_map[tid].store(pthread_self(), Ordering::Release);

        // Wait until all threads are ready.
        self.barrier.wait();
        Guard::register(self, tid)
    }

    pub fn reset_registrations(&mut self) {
        self.registered_count.store(0, Ordering::SeqCst);
        self.barrier = Barrier::new(self.num_threads);
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        for i in 0..self.num_threads {
            let retired = &mut self.threads[i].retired;
            while !retired.is_empty() {
                unsafe { retired.remove().deallocate() };
            }
        }
    }
}

unsafe impl Send for Collector {}
unsafe impl Sync for Collector {}

pub struct Guard {
    collector: *mut Collector,
    tid: ThreadId,
}

impl Guard {
    fn register(collector: &Collector, tid: ThreadId) -> Self {
        Self {
            collector: collector as *const _ as _,
            tid,
        }
    }

    #[inline(always)]
    #[allow(clippy::mut_from_ref)]
    fn coll_mut(&self) -> &mut Collector {
        unsafe { &mut *self.collector }
    }

    #[inline(always)]
    #[allow(clippy::mut_from_ref)]
    fn thread_mut(&self) -> &mut Thread {
        &mut self.coll_mut().threads[self.tid]
    }

    /// Start read phase.
    ///
    /// In read phase, programmers must aware following restrictions.
    ///
    /// 1. Reading global variables is permitted and reading shared
    ///    records is permitted if pointers to them were obtained
    ///    during this phase.
    ///   - e.g., by traversing a sequence of shared objects by
    ///     following pointers starting from a global variable—i.e., a root
    ///
    /// 2. Writes/CASs to shared records, writes/CASs to shared globals,
    ///    and system calls, are **not permitted.**
    ///
    /// To understand the latter restriction, suppose an operation
    /// allocates a node using malloc during its read phase, and before
    /// it uses the node, the thread performing the operation is
    /// neutralized. This would cause **a memory leak.**
    ///
    /// Additionally, writes to thread local data structures are
    /// not recommended. To see why, suppose a thread maintains
    /// a thread local doubly-linked list, and also updates this list
    /// as part of the read phase of some operation on the shared data
    /// structure. If the thread is neutralized in middle of its update
    /// to this local list, it might corrupt the structure of the list.
    #[inline]
    pub fn start_read(&self) {
        // `sigsetjmp` must called first. (in `set_checkpoint()`)
        // Since, if `sigsetjmp` is done later than other jobs
        // in `start_read`, the restartable would never be set to 1
        // and the upgrade assert will fail.
        unsafe { recovery::set_checkpoint() };
        let thread = &mut self.coll_mut().threads[self.tid];
        assert!(
            !recovery::is_restartable(),
            "restartable value should be false before starting read phase"
        );

        thread.proposed_len.store(0, Ordering::Release);
        recovery::set_restartable(true);
    }

    /// Prevent other threads from deleting the record.
    #[inline]
    pub fn protect<T>(&self, ptr: *mut T) {
        let thread = self.thread_mut();
        let hazptr_len = thread.proposed_len.load(Ordering::Acquire);
        if hazptr_len == thread.proposed_hazptrs.len() {
            panic!("The hazard pointer bag is already full.");
        }
        thread.proposed_hazptrs[hazptr_len].store(ptr as *mut _, Ordering::Release);
        thread.proposed_len.store(hazptr_len + 1, Ordering::Release);
    }

    /// End read phase.
    ///
    /// Note that it is also equivalent to upgrading to write phase.
    #[inline]
    pub fn end_read(&self) {
        recovery::set_restartable(false);
    }

    #[inline]
    pub unsafe fn retire<T>(&self, ptr: *mut T) {
        let collector = self.coll_mut();
        let num_threads = collector.num_threads;

        if self.thread_mut().is_out_of_patience() {
            // Tell other threads that I'm starting signaling.
            self.thread_mut()
                .announced_ts
                .fetch_add(1, Ordering::SeqCst);

            collector.restart_all_threads(self.tid);
            // Tell other threads that I have done signaling.
            self.thread_mut()
                .announced_ts
                .fetch_add(1, Ordering::SeqCst);

            // Full bag shall be reclaimed so clear any bag head.
            // Avoiding changes to arg of this reclaim_freeable.
            self.thread_mut().saved_block_head = null_mut();
            collector.reclaim_freeable(self.tid);

            self.thread_mut().first_lo_entry_flag = true;
            self.thread_mut().retires_since_lo_watermark = 0;

            for i in 0..num_threads {
                self.thread_mut().saved_ts[i] = 0;
            }
        } else if self.thread_mut().is_past_lo_watermark() {
            // On the first entry to lo-path, I shall save my baghead.
            // Up to this baghead, I can reclaim upon detecting that someone
            // has started and finished signalling after I saved Baghead.
            // That is a condition where all threads have gone Quiescent
            // at least once after I saved my baghead.
            if self.thread_mut().first_lo_entry_flag {
                self.thread_mut().first_lo_entry_flag = false;
                self.thread_mut().set_lo_watermark();

                // Take a relaxed snapshot of all other announce_ts,
                // to be used to know if its time to reclaim at lo-path.
                for i in 0..num_threads {
                    self.thread_mut().saved_ts[i] =
                        collector.threads[i].announced_ts.load(Ordering::Relaxed);
                }
            }

            for i in 0..num_threads {
                if collector.threads[i].announced_ts.load(Ordering::Relaxed)
                    >= self.thread_mut().saved_ts[i] + 2
                {
                    // If the baghead is not `None`, then reclamation shall happen
                    // from the baghead to tail in functions depicting reclamation of lo-watermark path.
                    collector.reclaim_freeable(self.tid);

                    self.thread_mut().first_lo_entry_flag = true;
                    self.thread_mut().retires_since_lo_watermark = 0;
                    for j in 0..num_threads {
                        self.thread_mut().saved_ts[j] = 0;
                    }
                    break;
                }
            }
        }

        if !self.thread_mut().first_lo_entry_flag {
            self.thread_mut().retires_since_lo_watermark += 1;
        }

        self.thread_mut().retired.add(ptr);
    }
}
