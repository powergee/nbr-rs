/// A thread-local recovery manager with signal handling
use nix::libc::{c_void, siginfo_t};
use nix::sys::pthread::{pthread_kill, Pthread};
use nix::sys::signal::{
    pthread_sigmask, sigaction, SaFlags, SigAction, SigHandler, SigSet, SigmaskHow, Signal,
};
use setjmp::{sigjmp_buf, siglongjmp, sigsetjmp};
use std::cell::RefCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{compiler_fence, fence, AtomicBool, Ordering};

static mut NEUTRALIZE_SIGNAL: Signal = Signal::SIGUSR1;
static mut SIG_ACTION: MaybeUninit<SigAction> = MaybeUninit::uninit();

thread_local! {
    static JMP_BUF: RefCell<MaybeUninit<sigjmp_buf>> = RefCell::new(MaybeUninit::uninit());
    static RESTARTABLE: RefCell<AtomicBool> = RefCell::new(AtomicBool::new(false));
}

/// Install a process-wide signal handler.
/// Note that we don't have to call `sigaction` for every child thread.
///
/// By default, SIGUSR1 is used as a neutralize signal.
/// To use the other signal, use `set_neutralize_signal`.
#[inline]
pub(crate) unsafe fn install() {
    let sig_action = SigAction::new(
        SigHandler::SigAction(handle_signal),
        // Restart any interrupted sys calls instead of silently failing
        SaFlags::SA_RESTART | SaFlags::SA_SIGINFO,
        // Block signals during handler
        SigSet::all(),
    );
    SIG_ACTION.write(sig_action);
    if sigaction(NEUTRALIZE_SIGNAL, SIG_ACTION.assume_init_ref()).is_err() {
        panic!("failed to install signal handler");
    }
}

#[inline]
pub(crate) unsafe fn send_signal(pthread: Pthread) -> nix::Result<()> {
    pthread_kill(pthread, NEUTRALIZE_SIGNAL)
}

#[inline]
pub(crate) unsafe fn set_checkpoint() {
    while unsafe { sigsetjmp(JMP_BUF.with(|buf| buf.borrow_mut().as_mut_ptr()), 0) } == 1 {
        fence(Ordering::SeqCst);
        let mut oldset = SigSet::empty();
        oldset.add(NEUTRALIZE_SIGNAL);
        if pthread_sigmask(SigmaskHow::SIG_UNBLOCK, Some(&oldset), None).is_err() {
            panic!("Failed to unblock signal");
        }
    }
}

#[inline]
pub(crate) fn is_restartable() -> bool {
    RESTARTABLE.with(|rest| rest.borrow().load(Ordering::Acquire))
}

#[inline]
pub(crate) fn set_restartable(set_rest: bool) {
    RESTARTABLE.with(|rest| rest.borrow().store(set_rest, Ordering::Release));
}

/// Set user-defined neutralize signal.
/// This function allows a user to use the other signal
/// than SIGUSR1 for a neutralize signal.
/// Note that it must called before creating
/// a Collector object.
///
/// # Safety
///
/// This function accesses and modify static variable.
/// To avoid potential race conditions, do not
/// call this function concurrently.
#[inline]
pub unsafe fn set_neutralize_signal(signal: Signal) {
    NEUTRALIZE_SIGNAL = signal;
}

extern "C" fn handle_signal(_: i32, _: *mut siginfo_t, _: *mut c_void) {
    if !is_restartable() {
        return;
    }

    let buf = JMP_BUF.with(|buf| buf.borrow_mut().as_mut_ptr());
    set_restartable(false);
    compiler_fence(Ordering::SeqCst);

    unsafe { siglongjmp(buf, 1) };
}
