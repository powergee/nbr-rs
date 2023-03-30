# NBR+ in Rust

A Rust crate which implements a concurrent garbage collector based on *NBR+* (Neutralization Based Reclamation) by Ajay Singh et al. [(arXiv:2012.14542v2)](https://arxiv.org/abs/2012.14542v2)

This library export `read_phase` macro, which represents the read phase(Φ𝑟𝑒𝑎𝑑) and handle neutralization signals when restarting is needed.

Like the well-known EBR library from [crossbeam](https://github.com/crossbeam-rs/crossbeam), you can crate shared `Collector` object and protect/retire nodes by per-thread `Guard`s.

# Example

You can see [an implementation of Harris's list](tests/list.rs) as an example.

## Encapsulating Read Phase

```rust
use nbr_rs::{read_phase, Collector, Guard};

let collector = &Collector::new(THREADS, MAX_HAZPTRS_PER_THREAD);

/* ... initializing a concurrent data structure */

spawn_thread(|| {
    // Thread-local guard to access the garbage collector.
    let guard = collector.register();

    // Traversing the data structure
    let node1, node2;
    read_phase!(guard, [node1, node2, ...] => {
        /* traversing codes for read phase */
    });
    /* At this point, all protectees are safe to dereference & write */

    if is_removed(node1) {
        // Retire unreachable nodes after using them.
        unsafe { guard.retire(node1) };
    }
})
```

# Compatibility

NBR+ scheme depends on POSIX(`setjmp`, `longjmp`) environment and atomic operations.

This library is tested and sanitized on x86 machines. However, as it follows memory ordering semantics from C++, other machines with different ISA would also work.