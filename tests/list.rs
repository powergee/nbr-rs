#![feature(strict_provenance_atomic_ptr)]

use nbr_rs::{Guard, Handle};
use std::cmp::Ordering::{Equal, Greater, Less};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::{mem, ptr};

/// Returns a bitmask containing the unused least significant bits of an aligned pointer to `T`.
#[inline]
const fn low_bits<T>() -> usize {
    (1 << mem::align_of::<T>().trailing_zeros()) - 1
}

/// Returns the pointer with the given tag
#[inline]
pub fn tagged<T>(ptr: *mut T, tag: usize) -> *mut T {
    ((ptr as usize & !low_bits::<T>()) | (tag & low_bits::<T>())) as *mut T
}

/// Decomposes a tagged pointer `data` into the pointer and the tag.
#[inline]
pub fn decompose_ptr<T>(ptr: *mut T) -> (*mut T, usize) {
    let ptr = ptr as usize;
    let raw = (ptr & !low_bits::<T>()) as *mut T;
    let tag = ptr & low_bits::<T>();
    (raw, tag)
}

/// Extract the actual address out of a tagged pointer
#[inline]
pub fn untagged<T>(ptr: *mut T) -> *mut T {
    let ptr = ptr as usize;
    (ptr & !low_bits::<T>()) as *mut T
}

/// Extracts the tag out of a tagged pointer
#[inline]
pub fn tag<T>(ptr: *mut T) -> usize {
    let ptr = ptr as usize;
    ptr & low_bits::<T>()
}

/// Some or executing the given expression.
macro_rules! some_or {
    ($e:expr, $err:expr) => {{
        match $e {
            Some(r) => r,
            None => $err,
        }
    }};
}

struct Node<K, V> {
    next: AtomicPtr<Node<K, V>>,
    key: K,
    value: V,
}

struct List<K, V> {
    head: AtomicPtr<Node<K, V>>,
}

impl<K, V> Drop for List<K, V> {
    fn drop(&mut self) {
        unsafe {
            let mut curr = self.head.load(Ordering::Relaxed);

            while let Some(curr_ref) = untagged(curr).as_ref() {
                let next = curr_ref.next.load(Ordering::Relaxed);
                drop(Box::from_raw(untagged(curr)));
                curr = next;
            }
        }
    }
}

impl<K, V> Node<K, V> {
    fn new(key: K, value: V) -> Self {
        Self {
            next: AtomicPtr::new(ptr::null_mut()),
            key,
            value,
        }
    }
}

struct Cursor<'g, K, V> {
    prev: *mut Node<K, V>,
    prev_link: &'g AtomicPtr<Node<K, V>>,
    prev_next: *mut Node<K, V>,
    curr: *mut Node<K, V>,
    found: bool,
}

impl<'g, K, V> Cursor<'g, K, V> {
    fn new(head: &'g AtomicPtr<Node<K, V>>) -> Self {
        let first = head.load(Ordering::Acquire);
        Self {
            prev: ptr::null_mut(),
            prev_link: head,
            prev_next: first,
            curr: first,
            found: false,
        }
    }
}

impl<'g, K, V> nbr_rs::Cursor for Cursor<'g, K, V> {
    fn protect_with(&self, guard: &mut Guard) {
        guard.protect(untagged(self.prev));
        guard.protect(untagged(self.curr));
    }
}

impl<K, V> List<K, V>
where
    K: Ord,
{
    pub fn new() -> Self {
        List {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    // Traverse the Harris List.
    // Clean up a chain of logically removed nodes in each traversal.
    fn search(&self, key: &K, handle: &mut Handle) -> Cursor<K, V> {
        loop {
            let cursor = handle.read_phase(|_| {
                let mut cursor = Cursor::new(&self.head);

                cursor.found = loop {
                    let curr_node =
                        some_or!(unsafe { untagged(cursor.curr).as_ref() }, break false);
                    let next = curr_node.next.load(Ordering::Acquire);

                    if tag(next) != 0 {
                        cursor.curr = tagged(next, 0);
                        continue;
                    }

                    match curr_node.key.cmp(key) {
                        Less => {
                            cursor.prev = cursor.curr;
                            cursor.prev_link = &curr_node.next;
                            cursor.prev_next = next;
                            cursor.curr = next;
                        }
                        Equal => break true,
                        Greater => break false,
                    }
                };
                cursor
            });

            if cursor.prev_next == cursor.curr {
                return cursor;
            }

            if cursor
                .prev_link
                .compare_exchange(
                    cursor.prev_next,
                    cursor.curr,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                continue;
            }

            let mut node = cursor.prev_next;
            while tagged(node, 0) != cursor.curr {
                let next = unsafe { &*untagged(node) }.next.load(Ordering::Acquire);
                unsafe { handle.retire(untagged(node)) };
                node = next;
            }

            return cursor;
        }
    }

    pub fn get<'g>(&'g self, key: &K, handle: &'g mut Handle) -> Option<&'g V> {
        let cursor = self.search(key, handle);
        if cursor.found {
            unsafe { cursor.curr.as_ref() }.map(|n| &n.value)
        } else {
            None
        }
    }

    pub fn insert(&self, key: K, value: V, handle: &mut Handle) -> Result<(), (K, V)> {
        let mut new_node = Box::new(Node::new(key, value));
        loop {
            let cursor = self.search(&new_node.key, handle);
            if cursor.found {
                return Err((new_node.key, new_node.value));
            }

            new_node.next.store(cursor.curr, Ordering::Relaxed);
            let new_node_ptr = Box::into_raw(new_node);

            match cursor.prev_link.compare_exchange(
                cursor.curr,
                new_node_ptr,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(_) => new_node = unsafe { Box::from_raw(new_node_ptr) },
            }
        }
    }

    pub fn remove<'g>(&'g self, key: &K, handle: &'g mut Handle) -> Option<&'g V> {
        loop {
            let cursor = self.search(key, handle);
            if !cursor.found {
                return None;
            }

            let curr_node = unsafe { &*cursor.curr };
            let next = curr_node.next.fetch_or(1, Ordering::AcqRel);
            if tag(next) == 1 {
                continue;
            }

            if cursor
                .prev_link
                .compare_exchange(cursor.curr, next, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                unsafe { handle.retire(cursor.curr) };
            }
            return Some(&curr_node.value);
        }
    }
}

/// Test with Harris List
#[cfg(test)]
mod tests {
    use super::List;
    extern crate rand;
    use crossbeam_utils::thread;
    use nbr_rs::Collector;
    use rand::prelude::*;

    const THREADS: usize = 30;
    const ELEMENTS_PER_THREADS: usize = 1000;

    #[test]
    fn smoke_list() {
        let map = &List::new();
        let collector = &Collector::new();

        thread::scope(|s| {
            for t in 0..THREADS {
                s.spawn(move |_| {
                    let mut guard = collector.register();
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<usize> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(map.insert(i, i.to_string(), &mut guard).is_ok());
                    }
                });
            }
        })
        .unwrap();

        thread::scope(|s| {
            for t in 0..THREADS {
                s.spawn(move |_| {
                    let mut guard = collector.register();
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<usize> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    if t < THREADS / 2 {
                        for i in keys {
                            assert_eq!(i.to_string(), *map.remove(&i, &mut guard).unwrap());
                        }
                    } else {
                        for i in keys {
                            assert_eq!(i.to_string(), *map.get(&i, &mut guard).unwrap());
                        }
                    }
                });
            }
        })
        .unwrap();
    }
}
