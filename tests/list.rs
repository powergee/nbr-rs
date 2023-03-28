#![feature(strict_provenance_atomic_ptr)]

use nbr_rs::{read_phase, Guard};
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

#[derive(Debug)]
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

struct Cursor<K, V> {
    prev: *mut Node<K, V>,
    curr: *mut Node<K, V>,
    found: bool,
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
    fn search(&self, key: &K, guard: &Guard) -> Cursor<K, V> {
        let mut cursor = Cursor {
            prev: ptr::null_mut(),
            curr: ptr::null_mut(),
            found: false,
        };
        let mut prev_next;

        'search_loop: loop {
            read_phase!(guard; untagged(cursor.prev), untagged(cursor.curr); {
                cursor.prev = &self.head as *const _ as *mut Node<K, V>;
                cursor.curr = self.head.load(Ordering::Acquire);
                prev_next = cursor.curr;

                cursor.found = loop {
                    let curr_node = some_or!(unsafe { untagged(cursor.curr).as_ref() }, break false);
                    let next = curr_node.next.load(Ordering::Acquire);

                    if tag(next) != 0 {
                        cursor.curr = tagged(next, 0);
                        continue;
                    }

                    match curr_node.key.cmp(key) {
                        Less => {
                            cursor.prev = cursor.curr;
                            cursor.curr = next;
                            prev_next = next;
                        }
                        Equal => break true,
                        Greater => break false,
                    }
                };
            });

            if prev_next == cursor.curr {
                return cursor;
            }

            let prev_ref = unsafe { &*untagged(cursor.prev) };
            if prev_ref
                .next
                .compare_exchange(prev_next, cursor.curr, Ordering::Release, Ordering::Relaxed)
                .is_err()
            {
                continue 'search_loop;
            }

            let mut node = prev_next;
            while tagged(node, 0) != cursor.curr {
                let next = unsafe { &*untagged(node) }.next.load(Ordering::Acquire);
                unsafe { guard.retire(untagged(node)) };
                node = next;
            }

            return cursor;
        }
    }

    pub fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        let cursor = self.search(key, guard);
        if cursor.found {
            unsafe { cursor.curr.as_ref() }.map(|n| &n.value)
        } else {
            None
        }
    }

    pub fn insert(&self, key: K, value: V, guard: &Guard) -> Result<(), (K, V)> {
        let mut new_node = Box::new(Node::new(key, value));
        loop {
            let cursor = self.search(&new_node.key, guard);
            if cursor.found {
                return Err((new_node.key, new_node.value));
            }

            new_node.next.store(cursor.curr, Ordering::Relaxed);
            let new_node_ptr = Box::into_raw(new_node);

            match unsafe { &*cursor.prev }.next.compare_exchange(
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

    pub fn remove<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        loop {
            let cursor = self.search(key, guard);
            if !cursor.found {
                return None;
            }

            let curr_node = unsafe { &*cursor.curr };
            let next = curr_node.next.fetch_or(1, Ordering::Acquire);
            if tag(next) == 1 {
                continue;
            }

            let prev_ref = unsafe { &*cursor.prev };
            if prev_ref
                .next
                .compare_exchange(cursor.curr, next, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                unsafe { guard.retire(cursor.curr) };
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
    use std::sync::Arc;

    const THREADS: usize = 30;
    const ELEMENTS_PER_THREADS: usize = 1000;

    #[test]
    fn smoke_list() {
        let map = &List::new();
        let collector = Arc::new(Collector::new(THREADS, 2));

        thread::scope(|s| {
            for t in 0..THREADS {
                let collector = Arc::clone(&collector);
                s.spawn(move |_| {
                    let guard = collector.register();
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<usize> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    for i in keys {
                        assert!(map.insert(i, i.to_string(), &guard).is_ok());
                    }
                });
            }
        })
        .unwrap();

        let mut collector = Arc::try_unwrap(collector).unwrap_or_else(|_| panic!());
        collector.reset_registrations();
        let collector = Arc::new(collector);

        thread::scope(|s| {
            for t in 0..THREADS {
                let collector = Arc::clone(&collector);
                s.spawn(move |_| {
                    let guard = collector.register();
                    let mut rng = rand::thread_rng();
                    let mut keys: Vec<usize> =
                        (0..ELEMENTS_PER_THREADS).map(|k| k * THREADS + t).collect();
                    keys.shuffle(&mut rng);
                    if t < THREADS / 2 {
                        for i in keys {
                            assert_eq!(i.to_string(), *map.remove(&i, &guard).unwrap());
                        }
                    } else {
                        for i in keys {
                            assert_eq!(i.to_string(), *map.get(&i, &guard).unwrap());
                        }
                    }
                });
            }
        })
        .unwrap();
    }
}
