//! A SQLite VFS which reads each page of a database from a remote source the first time SQLite uses it, while the
//! application fills the other pages in the background.
//!
//! The library is both a SQLite loadable extension, which registers the VFS in the SQLite that loads it, and a Node.js
//! addon, which registers the databases and fills their pages. Node.js does not export the functions of the SQLite it
//! embeds, so node:sqlite must load the library as an extension (`DatabaseSync#loadExtension()`) before a database is
//! registered. Both loads share one copy of the library, and so its state.

#[cfg(not(unix))]
compile_error!("The paged VFS supports Unix systems only.");

mod binding;
mod paged;
mod vfs;

use std::ffi::{c_char, c_int};

use libsqlite3_sys as ffi;

/// Called by `sqlite3_load_extension()`. The VFS stays registered after the connection which loaded it is closed.
///
/// # Safety
///
/// Must only be called by SQLite, with its API table.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_extension_init(
    _db: *mut ffi::sqlite3,
    _error: *mut *mut c_char,
    api: *const ffi::sqlite3_api_routines,
) -> c_int {
    match unsafe { vfs::install(api) } {
        Ok(()) => ffi::SQLITE_OK_LOAD_PERMANENTLY,
        Err(code) => code,
    }
}
