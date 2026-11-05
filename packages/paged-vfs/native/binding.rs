//! The Node.js API.

use std::sync::Arc;
use std::time::Duration;

use napi::bindgen_prelude::{Uint8Array, Uint32Array, Unknown};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{Error, Result, Status};
use napi_derive::napi;

use crate::paged::{self, PagedDb};
use crate::vfs;

// Called with the number of a missing page. Weak, so that it does not keep the event loop of its thread alive.
type OnRequest = ThreadsafeFunction<u32, Unknown<'static>, u32, Status, false, true>;

const DEFAULT_TIMEOUT: u32 = 30_000;

#[napi(object)]
pub struct RegisterOptions {
    /// The path of the database file. It is created, or truncated, to `pageCount` pages.
    pub path: String,
    pub page_size: u32,
    pub page_count: u32,
    /// How long a read waits for a missing page before failing, in milliseconds.
    pub timeout: Option<u32>,
}

/// Whether the extension registered the VFS in this process.
#[napi]
pub fn is_loaded() -> bool {
    vfs::installed().is_some()
}

#[napi]
pub fn vfs_name() -> String {
    vfs::NAME.to_string_lossy().into_owned()
}

/// Registers a database whose pages are all missing. `onRequest` is called on the current thread, which must not read
/// the database while it is incomplete.
#[napi]
pub fn register(options: RegisterOptions, on_request: OnRequest) -> Result<PagedDatabase> {
    let timeout = Duration::from_millis(u64::from(options.timeout.unwrap_or(DEFAULT_TIMEOUT)));
    let requester = Box::new(move |pgno| on_request.call(pgno, ThreadsafeFunctionCallMode::NonBlocking) == Status::Ok);
    let db = paged::register(&options.path, options.page_size, options.page_count, timeout, requester)
        .map_err(Error::from_reason)?;

    Ok(PagedDatabase { db })
}

#[napi]
pub struct PagedDatabase {
    db: Arc<PagedDb>,
}

#[napi]
impl PagedDatabase {
    #[napi(getter)]
    pub fn page_size(&self) -> u32 {
        self.db.page_size()
    }

    /// The number of pages of the database, which only changes when SQLite truncates it.
    #[napi(getter)]
    pub fn page_count(&self) -> u32 {
        self.db.page_count()
    }

    #[napi(getter)]
    pub fn missing(&self) -> u32 {
        self.db.missing()
    }

    /// Whether every page is present. The database is then unregistered, and reads are local only.
    #[napi(getter)]
    pub fn complete(&self) -> bool {
        self.db.is_complete()
    }

    /// Writes a page unless it is already present. Returns whether it was written.
    #[napi]
    pub fn fill(&self, pgno: u32, page: Uint8Array) -> Result<bool> {
        self.db.fill(pgno, &page).map_err(Error::from_reason)
    }

    /// Fails the reads waiting for a page.
    #[napi]
    pub fn fail(&self, pgno: u32) {
        self.db.fail(pgno);
    }

    /// The numbers of the missing pages, in ascending order.
    #[napi]
    pub fn missing_pages(&self) -> Uint32Array {
        Uint32Array::new(self.db.missing_pages())
    }

    /// Unregisters the database. Reads of missing pages fail from now on.
    #[napi]
    pub fn close(&self) {
        self.db.close();
    }
}
