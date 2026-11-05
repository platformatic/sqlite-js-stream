//! The registered databases, and the state of their pages.
//!
//! A registered database is a sparse file with the final size of the database. Each page is missing until the
//! application fills it, or SQLite writes it. Reading a missing page asks the application for it, and waits until it is
//! filled. Pages are never filled twice, so that filling an old version of a page never overwrites a newer one written
//! by SQLite.

use std::collections::HashMap;
use std::ffi::{CStr, CString, OsStr, c_char, c_int};
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, MutexGuard, PoisonError};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use libsqlite3_sys as ffi;

use crate::vfs;

/// Asks the application for a page. Returns false if the request could not be sent.
pub type Requester = Box<dyn Fn(u32) -> bool + Send + Sync>;

/// Identifies a file by device and inode, whatever the path used to open it.
type FileId = (u64, u64);

// SQLite never reads nor writes the page which contains the lock byte, at 1 GiB
const PENDING_BYTE: u64 = 0x4000_0000;

static REGISTRY: LazyLock<Mutex<HashMap<FileId, Arc<PagedDb>>>> = LazyLock::new(Default::default);
static REGISTERED: AtomicUsize = AtomicUsize::new(0);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // The state is consistent after every operation, even one that panicked
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn file_id(path: &Path) -> std::io::Result<FileId> {
    let metadata = fs::metadata(path)?;
    Ok((metadata.dev(), metadata.ino()))
}

/// Returns the registered database at the path opened by SQLite, if any.
pub fn lookup(name: &CStr) -> Option<Arc<PagedDb>> {
    if REGISTERED.load(Ordering::Acquire) == 0 {
        return None;
    }

    let id = file_id(Path::new(OsStr::from_bytes(name.to_bytes()))).ok()?;
    lock(&REGISTRY).get(&id).cloned()
}

/// Creates (or truncates) the database file to `page_count` pages, all missing, and registers it.
pub fn register(
    path: &str,
    page_size: u32,
    page_count: u32,
    timeout: Duration,
    requester: Requester,
) -> Result<Arc<PagedDb>, String> {
    if !page_size.is_power_of_two() || !(512..=65536).contains(&page_size) {
        return Err(format!(
            "Invalid page size {page_size}: it must be a power of two between 512 and 65536."
        ));
    }

    if page_count == 0 {
        return Err("The database must have at least one page.".into());
    }

    let file = FillFile::open(path)?;
    let id = file_id(Path::new(path)).map_err(|err| format!("Cannot read the metadata of \"{path}\": {err}"))?;
    let mut registry = lock(&REGISTRY);

    if registry.contains_key(&id) {
        return Err(format!("The database \"{path}\" is already registered."));
    }

    file.truncate(i64::from(page_count) * i64::from(page_size))
        .map_err(|code| format!("Cannot resize the database \"{path}\" (SQLite error {code})."))?;

    let mut state = State {
        page_count,
        present: Bitmap::new(page_count),
        requested: Bitmap::new(page_count),
        missing: page_count,
        failures: HashMap::new(),
        closed: false,
        file: Some(file),
        requester: Some(requester),
    };

    let lock_page = u32::try_from(PENDING_BYTE / u64::from(page_size) + 1).unwrap_or(u32::MAX);
    state.mark_present(lock_page);

    let db = Arc::new(PagedDb {
        id,
        page_size,
        timeout,
        fetcher: thread::current().id(),
        complete: AtomicBool::new(false),
        state: Mutex::new(state),
        changed: Condvar::new(),
    });

    registry.insert(id, Arc::clone(&db));
    REGISTERED.fetch_add(1, Ordering::AcqRel);
    drop(registry);

    let state = lock(&db.state);
    db.finish_if_complete(state);
    Ok(db)
}

pub struct PagedDb {
    id: FileId,
    page_size: u32,
    timeout: Duration,
    // The thread which fills the pages. It must never wait for a page, or it would wait for itself.
    fetcher: ThreadId,
    complete: AtomicBool,
    state: Mutex<State>,
    changed: Condvar,
}

struct State {
    page_count: u32,
    present: Bitmap,
    requested: Bitmap,
    missing: u32,
    // Number of failures to fetch each page, so that every reader waiting for a page sees a failure
    failures: HashMap<u32, u64>,
    closed: bool,
    file: Option<FillFile>,
    requester: Option<Requester>,
}

impl State {
    // Pages past the end of the database (after a truncation) are not missing
    fn has(&self, pgno: u32) -> bool {
        pgno == 0 || pgno > self.page_count || self.present.get(pgno)
    }

    fn mark_present(&mut self, pgno: u32) {
        if !self.has(pgno) {
            self.present.set(pgno);
            self.requested.clear(pgno);
            self.missing -= 1;
        }
    }

    fn failures(&self, pgno: u32) -> u64 {
        self.failures.get(&pgno).copied().unwrap_or(0)
    }
}

impl PagedDb {
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    pub fn page_count(&self) -> u32 {
        lock(&self.state).page_count
    }

    pub fn missing(&self) -> u32 {
        lock(&self.state).missing
    }

    pub fn is_complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }

    pub fn missing_pages(&self) -> Vec<u32> {
        let state = lock(&self.state);
        (1..=state.page_count).filter(|&pgno| !state.has(pgno)).collect()
    }

    /// Waits until the pages covered by a read are present.
    pub fn ensure(&self, offset: i64, amount: i32) -> Result<(), c_int> {
        if self.is_complete() || amount <= 0 || offset < 0 {
            return Ok(());
        }

        let (first, last) = self.pages(offset, amount);

        for pgno in first..=last {
            self.ensure_page(pgno).map_err(|_| ffi::SQLITE_IOERR_READ)?;
        }

        Ok(())
    }

    /// Writes with `write`, then marks the pages it covered present. A write which covers part of a missing page waits
    /// for the rest of it first.
    pub fn write(&self, offset: i64, amount: i32, write: impl FnOnce() -> c_int) -> c_int {
        if amount <= 0 || offset < 0 {
            return write();
        }

        let page_size = i64::from(self.page_size);
        let end = offset + i64::from(amount);
        let (first, last) = self.pages(offset, amount);

        if offset % page_size != 0 && self.ensure_page(first).is_err() {
            return ffi::SQLITE_IOERR_WRITE;
        }

        if end % page_size != 0 && self.ensure_page(last).is_err() {
            return ffi::SQLITE_IOERR_WRITE;
        }

        // Filling takes the same lock, so it can never overwrite this write
        let mut state = lock(&self.state);
        let code = write();

        if code == ffi::SQLITE_OK {
            for pgno in first..=last.min(state.page_count) {
                state.mark_present(pgno);
            }

            self.finish_if_complete(state);
            self.changed.notify_all();
        }

        code
    }

    /// Records that SQLite truncated the database: the pages past its end are not missing anymore.
    pub fn truncated(&self, size: i64) {
        let page_size = i64::from(self.page_size);
        let count = u32::try_from((size.max(0) + page_size - 1) / page_size).unwrap_or(u32::MAX);
        let mut state = lock(&self.state);

        if count < state.page_count {
            for pgno in count + 1..=state.page_count {
                state.mark_present(pgno);
            }

            state.page_count = count;
            self.finish_if_complete(state);
            self.changed.notify_all();
        }
    }

    /// Writes a page unless it is present. Returns whether it was written.
    pub fn fill(&self, pgno: u32, data: &[u8]) -> Result<bool, String> {
        if data.len() != self.page_size as usize {
            return Err(format!(
                "Page {pgno} has {} bytes instead of {}.",
                data.len(),
                self.page_size
            ));
        }

        let mut state = lock(&self.state);

        if state.closed {
            return Err("The database is closed.".into());
        }

        if state.has(pgno) {
            return Ok(false);
        }

        let offset = i64::from(pgno - 1) * i64::from(self.page_size);
        let Some(file) = state.file.as_ref() else {
            return Ok(false);
        };

        file.write(data, offset)
            .map_err(|code| format!("Cannot write page {pgno} (SQLite error {code})."))?;
        state.mark_present(pgno);
        self.finish_if_complete(state);
        self.changed.notify_all();
        Ok(true)
    }

    /// Fails the reads waiting for a page. The next read of the page asks for it again.
    pub fn fail(&self, pgno: u32) {
        let mut state = lock(&self.state);
        state.requested.clear(pgno);
        *state.failures.entry(pgno).or_insert(0) += 1;
        drop(state);
        self.changed.notify_all();
    }

    /// Unregisters the database. Reads of missing pages fail from now on.
    pub fn close(&self) {
        let mut state = lock(&self.state);
        state.closed = true;
        state.file = None;
        state.requester = None;
        drop(state);

        self.unregister();
        self.changed.notify_all();
    }

    fn pages(&self, offset: i64, amount: i32) -> (u32, u32) {
        let page_size = i64::from(self.page_size);
        let page = |offset: i64| u32::try_from(offset / page_size + 1).unwrap_or(u32::MAX);
        (page(offset), page(offset + i64::from(amount) - 1))
    }

    fn ensure_page(&self, pgno: u32) -> Result<(), ()> {
        let mut state = lock(&self.state);

        if state.has(pgno) {
            return Ok(());
        }

        if state.closed || thread::current().id() == self.fetcher {
            return Err(());
        }

        let failures = state.failures(pgno);
        let deadline = Instant::now() + self.timeout;

        loop {
            if !state.requested.get(pgno) {
                if !state.requester.as_ref().is_some_and(|request| request(pgno)) {
                    return Err(());
                }

                state.requested.set(pgno);
            }

            let now = Instant::now();

            if now >= deadline {
                // The next read asks again
                state.requested.clear(pgno);
                return Err(());
            }

            state = self
                .changed
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;

            if state.has(pgno) {
                return Ok(());
            }

            if state.closed || state.failures(pgno) != failures {
                return Err(());
            }
        }
    }

    // Once every page is present, the database is a regular SQLite database: reads skip the bitmap, and the
    // connections opened from now on do not use it.
    fn finish_if_complete(&self, mut state: MutexGuard<'_, State>) {
        if state.missing > 0 || self.is_complete() {
            return;
        }

        self.complete.store(true, Ordering::Release);
        state.file = None;
        state.requester = None;
        drop(state);

        self.unregister();
    }

    fn unregister(&self) {
        let mut registry = lock(&REGISTRY);

        if registry.get(&self.id).is_some_and(|db| ptr::eq(Arc::as_ptr(db), self)) {
            registry.remove(&self.id);
            REGISTERED.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// The file used to fill the pages, opened with the default VFS. Unlike a file opened by the application, closing it
/// cannot release the POSIX locks that the connections of the process hold on the database: the default VFS keeps
/// its descriptor open until they are released.
struct FillFile {
    file: *mut ffi::sqlite3_file,
    storage: *mut [u64],
    // Created by sqlite3_create_filename(), as xOpen() requires
    name: *const c_char,
}

// Only used with the lock of the database state held
unsafe impl Send for FillFile {}

impl FillFile {
    fn open(path: &str) -> Result<Self, String> {
        let installed = vfs::installed().ok_or("The paged VFS is not loaded.")?;
        let base = installed.base;
        let path_c = CString::new(path).map_err(|_| format!("Invalid path \"{path}\"."))?;

        unsafe {
            let max = usize::try_from((*base).mxPathname).unwrap_or(0) + 1;
            let mut full = vec![0 as c_char; max];
            let full_pathname = (*base).xFullPathname.ok_or("The default VFS has no xFullPathname().")?;
            let code = full_pathname(base, path_c.as_ptr(), max as c_int, full.as_mut_ptr());

            // SQLITE_OK_SYMLINK is a success
            if code & 0xff != ffi::SQLITE_OK {
                return Err(format!("Cannot resolve the path \"{path}\" (SQLite error {code})."));
            }

            let full = CStr::from_ptr(full.as_ptr()).to_owned();
            let journal = CString::new([full.as_bytes(), b"-journal"].concat()).unwrap_or_default();
            let wal = CString::new([full.as_bytes(), b"-wal"].concat()).unwrap_or_default();
            let name = (installed.create_filename)(full.as_ptr(), journal.as_ptr(), wal.as_ptr(), 0, ptr::null_mut());

            if name.is_null() {
                return Err("Out of memory.".into());
            }

            let words = usize::try_from((*base).szOsFile).unwrap_or(0).div_ceil(8).max(1);
            let storage = Box::into_raw(vec![0u64; words].into_boxed_slice());
            let fill_file = FillFile {
                file: storage.cast(),
                storage,
                name,
            };
            let base_open = (*base).xOpen.ok_or("The default VFS has no xOpen().")?;
            let flags = ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE | ffi::SQLITE_OPEN_MAIN_DB;
            let mut out_flags = 0;
            let code = base_open(base, name, fill_file.file, flags, &mut out_flags);

            if code != ffi::SQLITE_OK {
                return Err(format!("Cannot open the database \"{path}\" (SQLite error {code})."));
            }

            Ok(fill_file)
        }
    }

    fn methods(&self) -> &ffi::sqlite3_io_methods {
        unsafe { &*(*self.file).pMethods }
    }

    fn write(&self, data: &[u8], offset: i64) -> Result<(), c_int> {
        let write = self.methods().xWrite.ok_or(ffi::SQLITE_IOERR_WRITE)?;
        let code = unsafe { write(self.file, data.as_ptr().cast(), data.len() as c_int, offset) };
        if code == ffi::SQLITE_OK { Ok(()) } else { Err(code) }
    }

    fn truncate(&self, size: i64) -> Result<(), c_int> {
        let truncate = self.methods().xTruncate.ok_or(ffi::SQLITE_IOERR_TRUNCATE)?;
        let code = unsafe { truncate(self.file, size) };
        if code == ffi::SQLITE_OK { Ok(()) } else { Err(code) }
    }
}

impl Drop for FillFile {
    fn drop(&mut self) {
        unsafe {
            if !(*self.file).pMethods.is_null()
                && let Some(close) = self.methods().xClose
            {
                close(self.file);
            }

            if let Some(installed) = vfs::installed() {
                (installed.free_filename)(self.name);
            }

            drop(Box::from_raw(self.storage));
        }
    }
}

/// One bit per page, from page 1.
struct Bitmap(Vec<u64>);

impl Bitmap {
    fn new(pages: u32) -> Self {
        Bitmap(vec![0; (pages as usize).div_ceil(64)])
    }

    // Page 0 does not exist: its index is past the end
    fn index(pgno: u32) -> (usize, u64) {
        let bit = pgno.checked_sub(1).map_or(usize::MAX, |bit| bit as usize);
        (bit / 64, 1 << (bit % 64))
    }

    fn get(&self, pgno: u32) -> bool {
        let (word, mask) = Self::index(pgno);
        self.0.get(word).is_some_and(|value| value & mask != 0)
    }

    fn set(&mut self, pgno: u32) {
        let (word, mask) = Self::index(pgno);

        if let Some(value) = self.0.get_mut(word) {
            *value |= mask;
        }
    }

    fn clear(&mut self, pgno: u32) {
        let (word, mask) = Self::index(pgno);

        if let Some(value) = self.0.get_mut(word) {
            *value &= !mask;
        }
    }
}
