//! The return conventions of the raw verbs, turned into [`Result`]s: every failure goes through
//! [`Error::os`] / [`Error::errno`], so `EOPNOTSUPP` is promoted to [`Error::Unsupported`]
//! uniformly, and every teardown verb is checked the same way.

use std::io;
use std::os::raw::c_int;

use crate::error::{Error, Result};

/// A verb that returns a handle, or null with `errno` set. Call it right after the verb: the
/// error is read from `errno` here.
#[inline]
pub(crate) fn nonnull<T>(ptr: *mut T, wrap: impl FnOnce(io::Error) -> Error) -> Result<*mut T> {
    if ptr.is_null() {
        Err(Error::os(io::Error::last_os_error(), wrap))
    } else {
        Ok(ptr)
    }
}

/// A verb that returns 0 on success and the `errno` value itself otherwise (the libibverbs
/// convention).
#[inline]
pub(crate) fn errno(ret: c_int, wrap: impl FnOnce(io::Error) -> Error) -> Result<()> {
    if ret == 0 {
        Ok(())
    } else {
        Err(Error::errno(ret, wrap))
    }
}

/// A verb that returns 0 on success and -1 with `errno` set otherwise (the librdmacm and libc
/// convention). Call it right after the verb: the error is read from `errno` here.
#[cfg(feature = "rdmacm")]
#[inline]
pub(crate) fn os(ret: c_int, wrap: impl FnOnce(io::Error) -> Error) -> Result<()> {
    if ret == 0 {
        Ok(())
    } else {
        Err(Error::os(io::Error::last_os_error(), wrap))
    }
}

/// Check the `errno` a teardown verb returned. The wrappers panic rather than leak the resource
/// (see the crate docs on resource cleanup): a refused teardown means a raw handle obtained
/// through an escape hatch still references it.
#[inline]
pub(crate) fn destroyed(verb: &str, errno: c_int) {
    if errno != 0 {
        let e = io::Error::from_raw_os_error(errno);
        panic!("{verb} failed: {e}");
    }
}
