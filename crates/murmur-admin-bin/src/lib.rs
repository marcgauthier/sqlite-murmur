//! murmur-admin CLI entry point, built as a staticlib.
//!
//! CMake links this archive (which provides `main`) together with
//! `libmurmur_admin.a` and the storage dependencies into the sourceless
//! `murmur-admin` executable. `main` takes argc/argv from the C runtime
//! directly (no `std::env::args`: without Rust's lang_start wrapper the
//! process args are only available through the C parameters).

#[cfg(not(test))]
use std::ffi::CStr;
#[cfg(not(test))]
use std::os::raw::{c_char, c_int};

/// Program entry point (C runtime contract: argc + argv). Compiled out
/// under `cfg(test)`: the test harness provides its own `main`.
#[cfg(not(test))]
#[unsafe(no_mangle)]
pub extern "C" fn main(argc: c_int, argv: *const *const c_char) -> c_int {
    let args: Vec<String> = if argc < 0 || argv.is_null() {
        Vec::new()
    } else {
        // SAFETY: argc NUL-terminated entries per the C contract.
        unsafe {
            std::slice::from_raw_parts(argv, argc as usize)
                .iter()
                .map(|p| {
                    if p.is_null() {
                        String::new()
                    } else {
                        CStr::from_ptr(*p).to_string_lossy().into_owned()
                    }
                })
                .collect()
        }
    };
    let mut out = String::new();
    let mut err = String::new();
    let code = murmur_admin::admin_main(&args, &mut out, &mut err);
    print!("{out}");
    eprint!("{err}");
    // Flush before returning: the C runtime will exit()/return normally.
    use std::io::Write;
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
    code
}
