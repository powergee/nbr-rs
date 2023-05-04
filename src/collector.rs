use std::{
    cell::{RefCell, RefMut},
    marker::PhantomData,
    ptr::null_mut,
    sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize},
};

use atomic::{fence, Atomic, Ordering};
use crossbeam_utils::CachePadded;
use nix::{
    errno::Errno,
    sys::pthread::{pthread_self, Pthread},
};
use rustc_hash::FxHashSet;
use static_assertions::const_assert;

use crate::handle::Handle;
use crate::recovery;

const INITIAL_HAZPTR_CAP: usize = 16;

#[cfg(not(sanitize = "address"))]
pub(crate) const MAX_RINGBAG_CAPACITY_POW2: usize = 256;
#[cfg(sanitize = "address")]
pub(crate) const MAX_RINGBAG_CAPACITY_POW2: usize = 8;

const_assert!(Atomic::<Pthread>::is_lock_free());

pub struct Collector {
    records: CachePadded<ThreadList>,
}

impl Collector {
    pub const fn new() -> Self {
        Self {
            records: CachePadded::new(ThreadList::new()),
        }
    }

    /// Collect all pointers which are protected by at least
    /// one of the participants.
    pub(crate) fn collect_protected_ptrs(&self, reader: &Thread) -> FxHashSet<*mut u8> {
        fence(Ordering::SeqCst);
        self.records
            .iter()
            .flat_map(|record| record.iter_hazptrs(reader))
            .collect()
    }

    /// Iterate all announced timestamps, which is used for *NBR+ signal optimization*.
    ///
    /// Note that the length of the timestamp iterator may get longer than before
    /// because of new participants. However it is guaranteed that:
    ///
    /// 1. The length of the returned iterator never gets shorter.
    /// 2. The threads that previously existed present at the same index.
    ///    That is, the newcomers are always appended at the end of the sequence.
    pub(crate) fn iter_announced_ts<'g>(&'g self) -> impl Iterator<Item = usize> + 'g {
        self.records.iter().map(|record| record.load_ts())
    }

    /// Register a local `Handle` as a participant.
    pub fn register<'c>(&'c self) -> Handle<'c> {
        // Install a signal handler to handle the neutralization signal.
        unsafe { recovery::install() };
        let pthread = pthread_self();
        Handle::new(self, self.records.acquire(pthread))
    }

    /// Unregister a local `Handle` by invalidating the inner `Thread`.
    pub(crate) fn unregister<'c>(&'c self, thread: &'c Thread) {
        thread.using.store(false, Ordering::Release);
    }

    pub(crate) unsafe fn restart_all_threads(&self, reclaimer: &Thread) -> Result<(), Errno> {
        let reclaimer_tid = reclaimer.owner.load(Ordering::Acquire);
        for thread in self.records.iter() {
            let other_tid = thread.owner.load(Ordering::Acquire);
            if reclaimer_tid != other_tid {
                recovery::send_signal(other_tid)?;
            }
        }
        Ok(())
    }
}

unsafe impl Send for Collector {}
unsafe impl Sync for Collector {}

/// A grow-only linked list for thread registration.
pub(crate) struct ThreadList {
    head: AtomicPtr<Thread>,
}

impl ThreadList {
    const fn new() -> Self {
        Self {
            head: AtomicPtr::new(null_mut()),
        }
    }

    fn iter<'g>(&'g self) -> ThreadIter<'g> {
        ThreadIter {
            curr: self.head.load(Ordering::Acquire),
            _marker: PhantomData,
        }
    }

    /// Acquire an empty slot for a new participant.
    ///
    /// If there is an available slot, it returns a reference to that slot.
    /// Otherwise, it tries to append a new slot at the end of the list,
    /// and if it succeeds, returns the allocated slot.
    pub fn acquire<'c>(&'c self, tid: Pthread) -> &'c Thread {
        let mut prev_link = &self.head;
        let thread = loop {
            match unsafe { prev_link.load(Ordering::Acquire).as_ref() } {
                Some(curr) => {
                    if !curr.using.load(Ordering::Acquire)
                        && curr
                            .using
                            .compare_exchange(false, true, Ordering::Release, Ordering::Relaxed)
                            .is_ok()
                    {
                        break curr;
                    }
                    prev_link = &curr.next;
                }
                None => {
                    let new_thread = Box::into_raw(Box::new(Thread::new(true)));
                    if prev_link
                        .compare_exchange(
                            null_mut(),
                            new_thread,
                            Ordering::Release,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        break unsafe { &*new_thread };
                    } else {
                        unsafe { drop(Box::from_raw(new_thread)) };
                    }
                }
            }
        };
        thread.owner.store(tid, Ordering::Release);
        thread
    }
}

impl Drop for ThreadList {
    fn drop(&mut self) {
        let mut curr = *self.head.get_mut();
        while !curr.is_null() {
            curr = *unsafe { Box::from_raw(curr) }.next.get_mut();
        }
    }
}

struct ThreadIter<'g> {
    curr: *const Thread,
    _marker: PhantomData<&'g ()>,
}

impl<'g> Iterator for ThreadIter<'g> {
    type Item = &'g Thread;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(curr_ref) = unsafe { self.curr.as_ref() } {
            self.curr = curr_ref.next.load(Ordering::Acquire);
            if curr_ref.using.load(Ordering::Acquire) {
                return Some(curr_ref);
            }
        }
        None
    }
}

pub(crate) struct Thread {
    next: AtomicPtr<Thread>,
    using: AtomicBool,
    owner: Atomic<Pthread>,
    // A special hazard pointer slot to protect `hazards`
    // before iterating its contents.
    hazptr_for_iter: AtomicPtr<u8>,
    // The owner thread may resize its hazard pointer array `hazards`
    // by allocating new vector and overwriting the pointer.
    // For this reason, it is important for reclaimer to protect `hazards`
    // before reading its contents to avoid use-after-free.
    hazards: AtomicPtr<Vec<AtomicPtr<u8>>>,
    // Used for NBR+ signal optimization
    announced_ts: AtomicUsize,
    // Retired records collected by a `retire` operation
    retired: RefCell<Vec<Retired>>,
}

impl Thread {
    fn new(using: bool) -> Self {
        let hazards_vec =
            Vec::from(unsafe { core::mem::zeroed::<[AtomicPtr<u8>; INITIAL_HAZPTR_CAP]>() });
        let hazards = AtomicPtr::new(Box::into_raw(Box::new(hazards_vec)));
        Self {
            next: AtomicPtr::new(null_mut()),
            using: AtomicBool::new(using),
            owner: unsafe { core::mem::zeroed() },
            hazptr_for_iter: AtomicPtr::new(null_mut()),
            hazards,
            announced_ts: AtomicUsize::new(0),
            retired: RefCell::new(Vec::with_capacity(MAX_RINGBAG_CAPACITY_POW2)),
        }
    }

    fn iter_hazptrs<'g>(&'g self, reader: &'g Thread) -> HazardIter<'g> {
        // Protect `hazards` properly before reading it.
        let mut ptr = self.hazards.load(Ordering::Relaxed);
        loop {
            reader
                .hazptr_for_iter
                .store(ptr as *mut _, Ordering::Release);
            fence(Ordering::SeqCst);
            let new_ptr = self.hazards.load(Ordering::Acquire);
            if ptr == new_ptr {
                break;
            }
            ptr = new_ptr;
        }

        HazardIter {
            curr: &self.hazptr_for_iter,
            hazards: unsafe { &*ptr },
            next_idx: 0,
        }
    }

    #[inline]
    pub(crate) fn slots<'g>(&'g self) -> &'g [AtomicPtr<u8>] {
        unsafe { &*self.hazards.load(Ordering::Acquire) }
    }

    #[inline]
    pub(crate) fn retired_mut<'g>(&'g self) -> RefMut<'g, Vec<Retired>> {
        self.retired.borrow_mut()
    }

    #[inline]
    pub(crate) fn load_ts(&self) -> usize {
        self.announced_ts.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn advance_ts(&self) {
        self.announced_ts.fetch_add(1, Ordering::AcqRel);
    }
}

impl Drop for Thread {
    fn drop(&mut self) {
        // `Thread` is dropped when `Collector` is dropped.
        // So, we can safely deallocate all retired records
        // because all threads have finished their jobs and exited.
        unsafe { drop(Box::from_raw(self.hazards.load(Ordering::Acquire))) };
        for ret in self.retired.take().drain(..) {
            unsafe { ret.deallocate() };
        }
    }
}

struct HazardIter<'g> {
    curr: &'g AtomicPtr<u8>,
    hazards: &'g [AtomicPtr<u8>],
    next_idx: usize,
}

impl<'g> Iterator for HazardIter<'g> {
    type Item = *mut u8;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_idx >= self.hazards.len() {
            return None;
        }
        let curr_ptr = self.curr.load(Ordering::Acquire);
        if self.next_idx < self.hazards.len() {
            self.curr = &self.hazards[self.next_idx];
            self.next_idx += 1;
        }
        Some(curr_ptr)
    }
}

pub(crate) struct Retired {
    ptr: *mut u8,
    deleter: unsafe fn(*mut u8),
}

impl Default for Retired {
    fn default() -> Self {
        Self {
            ptr: null_mut(),
            deleter: free::<u8>,
        }
    }
}

impl Retired {
    pub fn new<T>(ptr: *mut T) -> Self {
        Self {
            ptr: ptr as *mut u8,
            deleter: free::<T>,
        }
    }

    pub fn ptr(&self) -> *mut u8 {
        self.ptr
    }

    pub unsafe fn deallocate(self) {
        (self.deleter)(self.ptr);
    }
}

unsafe fn free<T>(ptr: *mut u8) {
    drop(Box::from_raw(ptr as *mut T));
}
