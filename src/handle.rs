use std::{
    marker::PhantomData,
    ptr::NonNull,
    sync::atomic::{compiler_fence, AtomicPtr},
};

use atomic::{fence, Ordering};
use nix::sys::signal::{pthread_sigmask, SigSet, SigmaskHow};
use rustc_hash::FxHashSet;

use crate::{
    block_bag::{BlockBag, BlockPool, BLOCK_SIZE},
    collector::Thread,
    recovery, stats, Collector,
};

#[allow(unused)]
const OPS_BEFORE_TRYRECLAIM_LOWATERMARK: usize = 32;
#[cfg(not(sanitize = "address"))]
const MAX_RINGBAG_CAPACITY_POW2: usize = 256;
#[cfg(sanitize = "address")]
const MAX_RINGBAG_CAPACITY_POW2: usize = 8;

const PATIENCE: usize = MAX_RINGBAG_CAPACITY_POW2 / BLOCK_SIZE;

const SYNC_RETIRED_COUNT_BY: usize = 32;

pub struct Handle<'c> {
    pub(crate) collector: &'c Collector,
    pub(crate) thread: &'c Thread,

    // Retired records collected by a delete operation
    retired: *mut BlockBag,
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
    pool: *mut BlockPool,

    // Used for NBR+ signal optimization
    saved_retired_head: Option<NonNull<u8>>,
    saved_ts: Vec<usize>,
    first_lo_entry_flag: bool,
    retires_since_lo_watermark: usize,

    retired_cnt_buff: usize,
}

impl<'c> Handle<'c> {
    pub(crate) fn new(collector: &'c Collector, thread: &'c Thread) -> Self {
        let pool = Box::into_raw(Box::<BlockPool>::default());
        Self {
            collector,
            thread,
            retired: Box::into_raw(Box::new(BlockBag::new(pool))),
            pool,
            saved_retired_head: None,
            saved_ts: Vec::new(),
            first_lo_entry_flag: true,
            retires_since_lo_watermark: 0,
            retired_cnt_buff: 0,
        }
    }

    #[inline(always)]
    fn retired_mut(&mut self) -> &mut BlockBag {
        unsafe { &mut *self.retired }
    }

    #[inline]
    fn incr_cnt_buff(&mut self) -> usize {
        self.retired_cnt_buff += 1;
        return self.retired_cnt_buff;
    }

    #[inline]
    fn flush_cnt_buff(&mut self) {
        stats::incr_garb(self.retired_cnt_buff);
        self.retired_cnt_buff = 0;
    }

    #[inline]
    fn is_out_of_patience(&mut self) -> bool {
        self.retired_mut().size_in_blocks() > PATIENCE
    }

    #[inline]
    fn is_past_lo_watermark(&mut self) -> bool {
        cfg_if::cfg_if! {
            // When sanitizing, reclaim more aggressively.
            if #[cfg(sanitize = "address")] {
                !self.retired_mut().is_empty()
            } else {
                (self.retired_mut().size_in_blocks() as f32
                    > (PATIENCE as f32 / 3.0f32))
                    && ((self.retires_since_lo_watermark % OPS_BEFORE_TRYRECLAIM_LOWATERMARK) == 0)
            }
        }
    }

    #[inline]
    fn set_lo_watermark(&mut self) {
        self.saved_retired_head = Some(NonNull::new(self.retired_mut().peek().ptr()).unwrap());
    }

    #[inline]
    fn send_freeable_to_pool(&mut self, scanned_hazptrs: FxHashSet<*mut u8>) {
        let mut spare_bag = BlockBag::new(self.pool);

        if let Some(saved) = self.saved_retired_head {
            let retired = self.retired_mut();
            // Reclaim due to a lo-watermark path
            while retired.peek().ptr() != saved.as_ptr() {
                let ret = retired.pop();
                spare_bag.push_retired(ret);
            }
        }

        // Deallocate freeable records.
        // Note that even if we are on a lo-watermark path,
        // we must check a pointer is protected or not anyway.
        let mut reclaimed = 0;
        let retired = self.retired_mut();
        while !retired.is_empty() {
            let ret = retired.pop();
            if scanned_hazptrs.contains(&ret.ptr()) {
                spare_bag.push_retired(ret);
            } else {
                reclaimed += 1;
                unsafe { ret.deallocate() };
            }
        }
        stats::decr_garb(reclaimed);

        // Add all collected but protected records back to `retired`
        while !spare_bag.is_empty() {
            retired.push_retired(spare_bag.pop());
        }
    }

    fn reclaim_freeable(&mut self) {
        let hazptrs = self.collector.collect_protected_ptrs(self.thread);
        self.send_freeable_to_pool(hazptrs);
    }

    /// Start read phase. `f` is the body of this read phase, and it must
    /// return a protectable `Cursor` to safely dereference on the out of
    /// this read phase.
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
    #[inline(never)]
    #[cfg(not(sanitize = "address"))]
    pub fn read_phase<F, P>(&self, f: F) -> P
    where
        F: Fn(&Phase) -> P,
        P: Cursor,
    {
        // Make a checkpoint with `sigsetjmp` for recovering in read phase.
        compiler_fence(Ordering::SeqCst);
        let buf = recovery::jmp_buf();
        if unsafe { setjmp::sigsetjmp(buf, 0) } == 1 {
            fence(Ordering::SeqCst);
            let mut oldset = SigSet::empty();
            oldset.add(unsafe { recovery::neutralize_signal() });
            if pthread_sigmask(SigmaskHow::SIG_UNBLOCK, Some(&oldset), None).is_err() {
                panic!("Failed to unblock signal");
            }
        }
        compiler_fence(Ordering::SeqCst);

        // Get ready to open the phase by setting `RESTARTABLE` to `true`.
        assert!(
            !recovery::is_restartable(),
            "restartable value should be false before starting read phase"
        );
        recovery::set_restartable(true);
        compiler_fence(Ordering::SeqCst);

        // Execute the body of this read phase and
        // protect the returned cursor.
        let phase = Phase::new(self);
        let cursor = f(&phase);
        cursor.protect_with(&mut Guard::new(self));
        fence(Ordering::SeqCst);

        // Finaly, close this read phase by unsetting the `RESTARTABLE`.
        recovery::set_restartable(false);
        fence(Ordering::SeqCst);

        cursor
    }

    /// A variant of `read_phase` for address-sanitizing.
    ///
    /// # HACK: "Why there is a dummy loop with `black_box`?"
    ///
    /// It is not needed in normal builds, but
    /// when address-sanitizing, the address sanitizer often gives
    /// a false positive by recognizing `longjmp` as
    /// stack buffer overflow (or stack corruption).
    ///
    /// However, awkwardly, if it wrapped by a loop block,
    /// it seems that the sanitizer recognizes `longjmp` as
    /// normal `continue` operation and totally satisfies with it.
    ///
    /// So, they are added to avoid false positives from the sanitizer.
    #[inline(never)]
    #[cfg(sanitize = "address")]
    pub fn read_phase<F, P>(&self, f: F) -> P
    where
        F: Fn(&Phase) -> P,
        P: Cursor,
    {
        // A dummy loop to avoid a false positive from address-sanitizer.
        loop {
            // Make a checkpoint with `sigsetjmp` for recovering in read phase.
            compiler_fence(Ordering::SeqCst);
            let buf = recovery::jmp_buf();
            if unsafe { setjmp::sigsetjmp(buf, 0) } == 1 {
                fence(Ordering::SeqCst);
                let mut oldset = SigSet::empty();
                oldset.add(unsafe { recovery::neutralize_signal() });
                if pthread_sigmask(SigmaskHow::SIG_UNBLOCK, Some(&oldset), None).is_err() {
                    panic!("Failed to unblock signal");
                }
            }
            compiler_fence(Ordering::SeqCst);

            // Get ready to open the phase by setting `RESTARTABLE` to `true`.
            assert!(
                !recovery::is_restartable(),
                "restartable value should be false before starting read phase"
            );
            recovery::set_restartable(true);
            compiler_fence(Ordering::SeqCst);

            // Execute the body of this read phase and
            // protect the returned cursor.
            let phase = Phase::new(self);
            let cursor = f(&phase);
            cursor.protect_with(&mut Guard::new(self));
            fence(Ordering::SeqCst);

            // Finaly, close this read phase by unsetting the `RESTARTABLE`.
            recovery::set_restartable(false);
            fence(Ordering::SeqCst);

            if core::intrinsics::black_box(true) {
                return cursor;
            }
        }
    }

    /// Retire a pointer.
    /// It may trigger other threads to restart.
    ///
    /// # Safety
    /// * The given memory block is no longer modified.
    /// * It is no longer possible to reach the block from
    ///   the data structure.
    /// * The same block is not retired more than once.
    pub unsafe fn retire<T>(&mut self, ptr: *mut T) {
        let cnt_buff = self.incr_cnt_buff();

        if self.is_out_of_patience() {
            // Tell other threads that I'm starting signaling.
            self.thread.advance_ts();
            compiler_fence(Ordering::SeqCst);

            if let Err(err) = self.collector.restart_all_threads(self.thread) {
                panic!("Failed to restart other threads: {err}");
            } else {
                // Tell other threads that I have done signaling.
                self.thread.advance_ts();
                self.flush_cnt_buff();

                // Full bag shall be reclaimed so clear any bag head.
                // Avoiding changes to arg of this reclaim_freeable.
                self.saved_retired_head = None;
                self.reclaim_freeable();

                self.first_lo_entry_flag = true;
                self.retires_since_lo_watermark = 0;
                self.saved_ts.clear();
            }
        } else if self.is_past_lo_watermark() {
            // On the first entry to lo-path, I shall save my baghead.
            // Up to this baghead, I can reclaim upon detecting that someone
            // has started and finished signalling after I saved Baghead.
            // That is a condition where all threads have gone Quiescent
            // at least once after I saved my baghead.
            if self.first_lo_entry_flag {
                self.first_lo_entry_flag = false;
                self.set_lo_watermark();

                // Take a snapshot of all other announce_ts,
                // to be used to know if its time to reclaim at lo-path.
                self.saved_ts = self
                    .collector
                    .iter_announced_ts()
                    .map(|ts| ts + (ts % 2))
                    .collect();
            }

            for (prev, curr) in self.saved_ts.iter().zip(self.collector.iter_announced_ts()) {
                if prev + 2 <= curr {
                    self.flush_cnt_buff();

                    // If the baghead is not `None`, then reclamation shall happen
                    // from the baghead to tail in functions depicting reclamation of lo-watermark path.
                    self.reclaim_freeable();

                    self.first_lo_entry_flag = true;
                    self.retires_since_lo_watermark = 0;
                    self.saved_ts.clear();
                    break;
                }
            }
        }

        if !self.first_lo_entry_flag {
            self.retires_since_lo_watermark += 1;
        }

        self.retired_mut().push(ptr);

        if cnt_buff % SYNC_RETIRED_COUNT_BY == 0 {
            self.flush_cnt_buff();
        }
    }
}

impl<'c> Drop for Handle<'c> {
    fn drop(&mut self) {
        unsafe {
            let mut retired = Box::from_raw(self.retired);
            retired.deallocate_all();
            drop(retired);
            drop(Box::from_raw(self.pool));
        }
        self.flush_cnt_buff();
        self.collector.unregister(&mut self.thread);
    }
}

/// A protectable `Cursor` trait which contains acquired pointers
/// while traversing through a data structure.
///
/// Thanks to the neutralization-based read phase, we don't have to
/// protect the pointer every time we load it. However, it is crucial
/// to protect all pointers to dereference right before exiting the read phase.
/// This trait requires to implement `protect_with` method, so that
/// NBR+ engine can offer provide protection to pointers from the acquired
/// cursor before returning from `read_phase` function.
pub trait Cursor {
    /// Protect raw pointers in this cursor to dereference them
    /// after the phase.
    fn protect_with(&self, guard: &mut Guard);
}

impl Cursor for () {
    fn protect_with(&self, _guard: &mut Guard) {}
}

/// Provides a `protect` function so that programmers
/// can protect pointers from thier cursor after the read phase.
pub struct Guard<'h> {
    slot_idx: usize,
    slots: &'h [AtomicPtr<u8>],
}

impl<'h> Guard<'h> {
    fn new<'c>(handle: &'h Handle<'c>) -> Self {
        Self {
            slot_idx: 0,
            slots: handle.thread.slots(),
        }
    }

    /// Prevent other threads from deleting the record
    /// by protecting the given raw pointer with a hazard pointer.
    pub fn protect<T>(&mut self, ptr: *mut T) {
        // Note that the validating and firing a barrier are not needed.
        // `fence(SeqCst)` or light barrier is issued once after protecting all.
        self.slots
            .get(self.slot_idx)
            .expect("The hazard slots for this `Handle` are not enough!")
            .store(ptr as _, Ordering::Release);
        self.slot_idx += 1;
    }
}

/// A convinient read phase controller.
pub struct Phase<'h> {
    _marker: PhantomData<&'h ()>,
}

impl<'h> Phase<'h> {
    fn new<'c>(_handle: &'h Handle<'c>) -> Self {
        Self {
            _marker: PhantomData,
        }
    }

    /// Restart this read phase by performing `siglongjmp`.
    pub fn restart() -> ! {
        unsafe { recovery::longjmp_manually() }
    }
}

#[cfg(test)]
mod tests {
    use super::recovery::is_restartable;
    use super::Collector;
    use crossbeam_utils::thread;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    const THREADS: usize = 60;

    #[test]
    fn restart_all() {
        let collector = &Collector::new();
        let started = &AtomicUsize::new(0);

        thread::scope(|s| {
            for _ in 0..THREADS {
                s.spawn(move |_| {
                    let handle = collector.register();
                    assert!(!is_restartable());

                    handle.read_phase(|_| {
                        if started.fetch_add(1, Ordering::SeqCst) < THREADS {
                            assert!(is_restartable());
                            loop {
                                std::thread::sleep(Duration::from_micros(1))
                            }
                        }
                    });
                });
            }

            let handle = collector.register();
            while started.load(Ordering::SeqCst) < THREADS {
                std::thread::sleep(Duration::from_micros(1));
            }
            unsafe { collector.restart_all_threads(handle.thread).unwrap() };
        })
        .unwrap();
    }
}
