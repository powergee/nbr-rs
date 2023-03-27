#[macro_export]
/// Automate starting, ending, protecting and barriering for read phase.
macro_rules! read_phase {
    ($guard:expr; $($record:expr),*; $($t:tt)*) => {
        ($guard).start_read();
        std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);

        { $($t)* }

        $(
            ($guard).protect($record);
        )*
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
        ($guard).end_read();
    };
}
