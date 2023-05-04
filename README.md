# NBR+ in Rust

A Rust crate which implements a concurrent garbage collector based on *NBR+* (Neutralization Based Reclamation) by Ajay Singh et al. [(arXiv:2012.14542v2)](https://arxiv.org/abs/2012.14542v2)

# Example

You can see [an implementation of Harris's list](tests/list.rs) as an example.

# Compatibility

NBR+ scheme depends on POSIX(`setjmp`, `longjmp`) environment and atomic operations.

This library is tested and sanitized on x86 machines. However, as it follows memory ordering semantics from C++, other machines with different ISA would also work.