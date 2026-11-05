//! The VFS. It wraps the default VFS of SQLite, and waits for the missing pages of the registered databases before
//! reading them.

use std::ffi::{CStr, c_char, c_int, c_void};
use std::mem::size_of;
use std::ptr;
use std::sync::{Arc, OnceLock};

use libsqlite3_sys as ffi;

use crate::paged::{self, PagedDb};

pub const NAME: &CStr = c"sqlite-js-stream-paged-vfs";

type CreateFilename =
    unsafe extern "C" fn(*const c_char, *const c_char, *const c_char, c_int, *mut *const c_char) -> *const c_char;
type FreeFilename = unsafe extern "C" fn(*const c_char);

/// The VFS registered in SQLite, with the functions of SQLite it uses. They are read from the API table of the SQLite
/// which loaded the extension: the library never links its own SQLite.
pub struct Installed {
    pub base: *mut ffi::sqlite3_vfs,
    pub create_filename: CreateFilename,
    pub free_filename: FreeFilename,
    // SQLite keeps a pointer to it
    _vfs: Box<ffi::sqlite3_vfs>,
}

// The default VFS and ours are never freed, and SQLite synchronizes the calls to the VFS
unsafe impl Send for Installed {}
unsafe impl Sync for Installed {}

static INSTALLED: OnceLock<Result<Installed, c_int>> = OnceLock::new();

/// Registers the VFS. Only the first call does it: the following ones return its result.
///
/// # Safety
///
/// `api` must be the API table passed by SQLite to the extension.
pub unsafe fn install(api: *const ffi::sqlite3_api_routines) -> Result<(), c_int> {
    match INSTALLED.get_or_init(|| unsafe { create(api) }) {
        Ok(_) => Ok(()),
        Err(code) => Err(*code),
    }
}

pub fn installed() -> Option<&'static Installed> {
    INSTALLED.get().and_then(|installed| installed.as_ref().ok())
}

unsafe fn create(api: *const ffi::sqlite3_api_routines) -> Result<Installed, c_int> {
    // The fields of the API table are only ever appended, so those used here are at the same offset in every SQLite
    // version that has them (3.32 and later). rusqlite_extension_init2() is not used, as it rejects SQLite versions
    // older than the headers of libsqlite3-sys.
    let api = unsafe { api.as_ref() }.ok_or(ffi::SQLITE_MISUSE)?;
    let (Some(vfs_find), Some(vfs_register), Some(create_filename), Some(free_filename)) =
        (api.vfs_find, api.vfs_register, api.create_filename, api.free_filename)
    else {
        return Err(ffi::SQLITE_ERROR);
    };

    let base = unsafe { vfs_find(ptr::null()) };
    let Some(base_vfs) = (unsafe { base.as_ref() }) else {
        return Err(ffi::SQLITE_ERROR);
    };

    // The other methods are those of the default VFS. They receive our VFS instead of theirs, which holds the same
    // pAppData and mxPathname, the only fields the default VFSes of SQLite read.
    let mut vfs = Box::new(unsafe { ptr::read(base) });
    vfs.iVersion = base_vfs.iVersion.min(3);
    vfs.szOsFile = c_int::try_from(FILE_OFFSET).map_err(|_| ffi::SQLITE_ERROR)? + base_vfs.szOsFile;
    vfs.pNext = ptr::null_mut();
    vfs.zName = NAME.as_ptr();
    vfs.xOpen = Some(open);

    let code = unsafe { vfs_register(&mut *vfs, 0) };

    if code != ffi::SQLITE_OK {
        return Err(code);
    }

    Ok(Installed {
        base,
        create_filename,
        free_filename,
        _vfs: vfs,
    })
}

/// The files opened by the VFS. The file of the default VFS follows, at FILE_OFFSET.
#[repr(C)]
struct PagedFile {
    base: ffi::sqlite3_file,
    // From Arc::into_raw(), or null if the file is not a registered database
    db: *const PagedDb,
}

const FILE_OFFSET: usize = size_of::<PagedFile>().next_multiple_of(8);

/// # Safety
///
/// `file` must be a file opened by the VFS.
unsafe fn inner(file: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    unsafe { file.cast::<u8>().add(FILE_OFFSET).cast() }
}

/// # Safety
///
/// `file` must be a file opened by the VFS.
unsafe fn methods<'a>(file: *mut ffi::sqlite3_file) -> (*mut ffi::sqlite3_file, &'a ffi::sqlite3_io_methods) {
    unsafe {
        let inner = inner(file);
        (inner, &*(*inner).pMethods)
    }
}

/// # Safety
///
/// `file` must be a file opened by the VFS.
unsafe fn database<'a>(file: *mut ffi::sqlite3_file) -> Option<&'a PagedDb> {
    unsafe { (*file.cast::<PagedFile>()).db.as_ref() }
}

unsafe extern "C" fn open(
    _vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    let Some(installed) = installed() else {
        return ffi::SQLITE_MISUSE;
    };

    unsafe {
        let paged_file = file.cast::<PagedFile>();
        (*paged_file).base.pMethods = ptr::null();
        (*paged_file).db = ptr::null();

        let inner = inner(file);
        (*inner).pMethods = ptr::null();

        let Some(base_open) = (*installed.base).xOpen else {
            return ffi::SQLITE_ERROR;
        };

        let code = base_open(installed.base, name, inner, flags, out_flags);

        // SQLite calls xClose() even after a failure, unless pMethods is null
        if (*inner).pMethods.is_null() {
            return code;
        }

        if code == ffi::SQLITE_OK
            && flags & ffi::SQLITE_OPEN_MAIN_DB != 0
            && !name.is_null()
            && let Some(db) = paged::lookup(CStr::from_ptr(name))
        {
            (*paged_file).db = Arc::into_raw(db);
        }

        (*paged_file).base.pMethods = &METHODS;
        code
    }
}

// Version 2: shared memory for WAL, but no xFetch(). SQLite must not map a missing page in memory, which would bypass
// xRead().
static METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 2,
    xClose: Some(close),
    xRead: Some(read),
    xWrite: Some(write),
    xTruncate: Some(truncate),
    xSync: Some(sync),
    xFileSize: Some(file_size),
    xLock: Some(lock),
    xUnlock: Some(unlock),
    xCheckReservedLock: Some(check_reserved_lock),
    xFileControl: Some(file_control),
    xSectorSize: Some(sector_size),
    xDeviceCharacteristics: Some(device_characteristics),
    xShmMap: Some(shm_map),
    xShmLock: Some(shm_lock),
    xShmBarrier: Some(shm_barrier),
    xShmUnmap: Some(shm_unmap),
    xFetch: None,
    xUnfetch: None,
};

unsafe extern "C" fn close(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let (inner, methods) = methods(file);
        let code = methods.xClose.map_or(ffi::SQLITE_OK, |close| close(inner));
        let paged_file = file.cast::<PagedFile>();

        if !(*paged_file).db.is_null() {
            drop(Arc::from_raw((*paged_file).db));
            (*paged_file).db = ptr::null();
        }

        code
    }
}

unsafe extern "C" fn read(file: *mut ffi::sqlite3_file, buffer: *mut c_void, amount: c_int, offset: i64) -> c_int {
    unsafe {
        if let Some(db) = database(file)
            && let Err(code) = db.ensure(offset, amount)
        {
            return code;
        }

        let (inner, methods) = methods(file);
        methods
            .xRead
            .map_or(ffi::SQLITE_IOERR_READ, |read| read(inner, buffer, amount, offset))
    }
}

unsafe extern "C" fn write(file: *mut ffi::sqlite3_file, buffer: *const c_void, amount: c_int, offset: i64) -> c_int {
    unsafe {
        let (inner, methods) = methods(file);
        let Some(base_write) = methods.xWrite else {
            return ffi::SQLITE_IOERR_WRITE;
        };

        match database(file) {
            Some(db) if !db.is_complete() => db.write(offset, amount, || base_write(inner, buffer, amount, offset)),
            _ => base_write(inner, buffer, amount, offset),
        }
    }
}

unsafe extern "C" fn truncate(file: *mut ffi::sqlite3_file, size: i64) -> c_int {
    unsafe {
        let (inner, methods) = methods(file);
        let code = methods
            .xTruncate
            .map_or(ffi::SQLITE_IOERR_TRUNCATE, |truncate| truncate(inner, size));

        if code == ffi::SQLITE_OK
            && let Some(db) = database(file)
        {
            db.truncated(size);
        }

        code
    }
}

unsafe extern "C" fn sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    unsafe {
        let (inner, methods) = methods(file);
        methods.xSync.map_or(ffi::SQLITE_IOERR_FSYNC, |sync| sync(inner, flags))
    }
}

unsafe extern "C" fn file_size(file: *mut ffi::sqlite3_file, size: *mut i64) -> c_int {
    unsafe {
        let (inner, methods) = methods(file);
        methods
            .xFileSize
            .map_or(ffi::SQLITE_IOERR_FSTAT, |file_size| file_size(inner, size))
    }
}

unsafe extern "C" fn lock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe {
        let (inner, methods) = methods(file);
        methods.xLock.map_or(ffi::SQLITE_IOERR_LOCK, |lock| lock(inner, level))
    }
}

unsafe extern "C" fn unlock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe {
        let (inner, methods) = methods(file);
        methods
            .xUnlock
            .map_or(ffi::SQLITE_IOERR_UNLOCK, |unlock| unlock(inner, level))
    }
}

unsafe extern "C" fn check_reserved_lock(file: *mut ffi::sqlite3_file, result: *mut c_int) -> c_int {
    unsafe {
        let (inner, methods) = methods(file);
        methods
            .xCheckReservedLock
            .map_or(ffi::SQLITE_IOERR_CHECKRESERVEDLOCK, |check| check(inner, result))
    }
}

unsafe extern "C" fn file_control(file: *mut ffi::sqlite3_file, op: c_int, arg: *mut c_void) -> c_int {
    unsafe {
        let (inner, methods) = methods(file);
        methods
            .xFileControl
            .map_or(ffi::SQLITE_NOTFOUND, |file_control| file_control(inner, op, arg))
    }
}

unsafe extern "C" fn sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let (inner, methods) = methods(file);
        methods.xSectorSize.map_or(4096, |sector_size| sector_size(inner))
    }
}

unsafe extern "C" fn device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let (inner, methods) = methods(file);
        methods
            .xDeviceCharacteristics
            .map_or(0, |device_characteristics| device_characteristics(inner))
    }
}

unsafe extern "C" fn shm_map(
    file: *mut ffi::sqlite3_file,
    region: c_int,
    size: c_int,
    extend: c_int,
    pointer: *mut *mut c_void,
) -> c_int {
    unsafe {
        let (inner, methods) = methods(file);
        methods.xShmMap.map_or(ffi::SQLITE_IOERR_SHMMAP, |shm_map| {
            shm_map(inner, region, size, extend, pointer)
        })
    }
}

unsafe extern "C" fn shm_lock(file: *mut ffi::sqlite3_file, offset: c_int, count: c_int, flags: c_int) -> c_int {
    unsafe {
        let (inner, methods) = methods(file);
        methods.xShmLock.map_or(ffi::SQLITE_IOERR_SHMLOCK, |shm_lock| {
            shm_lock(inner, offset, count, flags)
        })
    }
}

unsafe extern "C" fn shm_barrier(file: *mut ffi::sqlite3_file) {
    unsafe {
        let (inner, methods) = methods(file);

        if let Some(shm_barrier) = methods.xShmBarrier {
            shm_barrier(inner);
        }
    }
}

unsafe extern "C" fn shm_unmap(file: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    unsafe {
        let (inner, methods) = methods(file);
        methods
            .xShmUnmap
            .map_or(ffi::SQLITE_OK, |shm_unmap| shm_unmap(inner, delete))
    }
}
