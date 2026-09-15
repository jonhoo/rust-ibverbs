//! The file-descriptor plumbing shared by the crate's event sources — completion channels, the
//! device's asynchronous events, and connection-manager channels: switching a descriptor to
//! non-blocking delivery, and waiting for it to become readable until a deadline.

use std::io;
use std::os::fd::BorrowedFd;
use std::time::{Duration, Instant};

use nix::poll::{poll, PollFd, PollFlags, PollTimeout};

/// Set `O_NONBLOCK` on `fd`, so reads report an empty queue (`EAGAIN`) instead of blocking.
pub(crate) fn set_nonblocking(fd: BorrowedFd<'_>) -> io::Result<()> {
    let flags = nix::fcntl::fcntl(fd, nix::fcntl::F_GETFL)?;
    let flags = nix::fcntl::OFlag::from_bits_retain(flags) | nix::fcntl::OFlag::O_NONBLOCK;
    nix::fcntl::fcntl(fd, nix::fcntl::FcntlArg::F_SETFL(flags))?;
    Ok(())
}

/// The instant `timeout` from now expires, or `None` for no deadline — also when the sum does not
/// fit an `Instant`, which only a timeout of many decades produces and means "wait forever" too.
pub(crate) fn deadline(timeout: Option<Duration>) -> Option<Instant> {
    timeout.and_then(|timeout| Instant::now().checked_add(timeout))
}

/// Block until `fd` is readable or `deadline` passes, returning whether it is readable.
///
/// A `poll(2)` interrupted by a signal (`EINTR`) is retried with the remaining time, and a wait
/// longer than `poll` can express is split into slices, so the deadline is honored exactly.
pub(crate) fn wait_readable(fd: BorrowedFd<'_>, deadline: Option<Instant>) -> io::Result<bool> {
    loop {
        let timeout = match deadline {
            None => PollTimeout::NONE,
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Ok(false);
                }
                // Round up to whole milliseconds, so the wait lasts at least the time asked for
                // rather than returning fractionally early, and cap at what `poll` accepts; a
                // longer wait simply polls again.
                let millis = remaining.as_nanos().div_ceil(1_000_000);
                let millis = u32::try_from(millis)
                    .unwrap_or(u32::MAX)
                    .min(i32::MAX as u32);
                PollTimeout::try_from(millis).expect("clamped to poll's maximum")
            }
        };
        match poll(&mut [PollFd::new(fd, PollFlags::POLLIN)], timeout) {
            // The slice elapsed; the next round decides whether the deadline has too.
            Ok(0) => continue,
            Ok(_) => return Ok(true),
            Err(nix::errno::Errno::EINTR) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}
