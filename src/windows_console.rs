//! Temporarily configure the Windows console for UTF-8 and virtual-terminal I/O.
//!
//! On non-Windows platforms the public guard is a no-op, so callers can use the
//! same API without platform-specific call sites. Dropping the guard performs
//! a best-effort restoration; call [`WindowsConsoleGuard::restore`] directly
//! when restoration errors need to be observed.

use std::fmt;

/// Error returned while configuring or restoring the console.
#[derive(Debug)]
pub struct WindowsConsoleError {
    operation: &'static str,
    detail: String,
}

#[cfg(windows)]
impl WindowsConsoleError {
    fn new(operation: &'static str, detail: impl Into<String>) -> Self {
        Self {
            operation,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for WindowsConsoleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} failed: {}", self.operation, self.detail)
    }
}

impl std::error::Error for WindowsConsoleError {}

// These masks are defined here, rather than relying on the platform bindings,
// so the pure helper can be tested on non-Windows hosts too.
#[cfg(any(windows, test))]
const ENABLE_VIRTUAL_TERMINAL_INPUT_MASK: u32 = 0x0200;
#[cfg(any(windows, test))]
const ENABLE_VIRTUAL_TERMINAL_PROCESSING_MASK: u32 = 0x0004;
#[cfg(any(windows, test))]
const DISABLE_NEWLINE_AUTO_RETURN_MASK: u32 = 0x0008;
#[cfg(windows)]
const UTF8_CODE_PAGE: u32 = 65001;

#[cfg(any(windows, test))]
const fn enable_vt_input(mode: u32) -> u32 {
    mode | ENABLE_VIRTUAL_TERMINAL_INPUT_MASK
}

#[cfg(any(windows, test))]
const fn enable_vt_output(mode: u32) -> u32 {
    mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING_MASK | DISABLE_NEWLINE_AUTO_RETURN_MASK
}

#[cfg(windows)]
mod platform {
    use super::{UTF8_CODE_PAGE, WindowsConsoleError, enable_vt_input, enable_vt_output};
    use windows::Win32::Foundation::GetLastError;
    use windows::Win32::System::Console::{
        CONSOLE_MODE, GetConsoleCP, GetConsoleMode, GetConsoleOutputCP, GetStdHandle,
        STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, SetConsoleCP, SetConsoleMode, SetConsoleOutputCP,
    };

    fn api_error(operation: &'static str, error: windows::core::Error) -> WindowsConsoleError {
        WindowsConsoleError::new(operation, error.to_string())
    }

    fn code_page_error(operation: &'static str) -> WindowsConsoleError {
        // GetConsoleCP and GetConsoleOutputCP report failure as zero, not a
        // Result. Capture the thread's last-error value immediately.
        let last_error = unsafe { GetLastError() };
        WindowsConsoleError::new(
            operation,
            format!("API returned 0 (GetLastError: {last_error:?})"),
        )
    }

    /// RAII guard that enables VT input/output and UTF-8 console code pages.
    ///
    /// The original console settings are restored on drop. Use [`Self::restore`]
    /// to receive any errors while restoring them explicitly.
    #[must_use = "dropping the guard immediately restores the original console settings"]
    pub struct WindowsConsoleGuard {
        input_handle: windows::Win32::Foundation::HANDLE,
        output_handle: windows::Win32::Foundation::HANDLE,
        input_mode: u32,
        output_mode: u32,
        input_code_page: u32,
        output_code_page: u32,
        input_mode_changed: bool,
        output_mode_changed: bool,
        input_code_page_changed: bool,
        output_code_page_changed: bool,
    }

    impl WindowsConsoleGuard {
        /// Enable VT input/output and set both console code pages to UTF-8.
        pub fn new() -> Result<Self, WindowsConsoleError> {
            let input_handle = unsafe { GetStdHandle(STD_INPUT_HANDLE) }
                .map_err(|error| api_error("GetStdHandle(stdin)", error))?;
            let output_handle = unsafe { GetStdHandle(STD_OUTPUT_HANDLE) }
                .map_err(|error| api_error("GetStdHandle(stdout)", error))?;

            let mut input_mode = CONSOLE_MODE(0);
            unsafe { GetConsoleMode(input_handle, &mut input_mode as *mut _) }
                .map_err(|error| api_error("GetConsoleMode(stdin)", error))?;
            let mut output_mode = CONSOLE_MODE(0);
            unsafe { GetConsoleMode(output_handle, &mut output_mode as *mut _) }
                .map_err(|error| api_error("GetConsoleMode(stdout)", error))?;

            let input_code_page = unsafe { GetConsoleCP() };
            if input_code_page == 0 {
                return Err(code_page_error("GetConsoleCP"));
            }
            let output_code_page = unsafe { GetConsoleOutputCP() };
            if output_code_page == 0 {
                return Err(code_page_error("GetConsoleOutputCP"));
            }

            let input_mode = input_mode.0;
            let output_mode = output_mode.0;
            let mut guard = Self {
                input_handle,
                output_handle,
                input_mode,
                output_mode,
                input_code_page,
                output_code_page,
                input_mode_changed: false,
                output_mode_changed: false,
                input_code_page_changed: false,
                output_code_page_changed: false,
            };

            unsafe {
                SetConsoleMode(
                    guard.input_handle,
                    CONSOLE_MODE(enable_vt_input(input_mode)),
                )
            }
            .map_err(|error| api_error("SetConsoleMode(stdin)", error))?;
            guard.input_mode_changed = true;

            unsafe {
                SetConsoleMode(
                    guard.output_handle,
                    CONSOLE_MODE(enable_vt_output(output_mode)),
                )
            }
            .map_err(|error| api_error("SetConsoleMode(stdout)", error))?;
            guard.output_mode_changed = true;

            unsafe { SetConsoleCP(UTF8_CODE_PAGE) }
                .map_err(|error| api_error("SetConsoleCP(UTF-8)", error))?;
            guard.input_code_page_changed = true;

            unsafe { SetConsoleOutputCP(UTF8_CODE_PAGE) }
                .map_err(|error| api_error("SetConsoleOutputCP(UTF-8)", error))?;
            guard.output_code_page_changed = true;

            Ok(guard)
        }

        /// Restore original settings, attempting every outstanding restoration.
        ///
        /// A failed restoration remains pending and will be retried by `Drop`.
        pub fn restore(&mut self) -> Result<(), WindowsConsoleError> {
            let mut first_error = None;

            if self.output_code_page_changed {
                match unsafe { SetConsoleOutputCP(self.output_code_page) } {
                    Ok(()) => self.output_code_page_changed = false,
                    Err(error) => {
                        first_error
                            .get_or_insert_with(|| api_error("SetConsoleOutputCP(restore)", error));
                    }
                }
            }
            if self.input_code_page_changed {
                match unsafe { SetConsoleCP(self.input_code_page) } {
                    Ok(()) => self.input_code_page_changed = false,
                    Err(error) => {
                        first_error
                            .get_or_insert_with(|| api_error("SetConsoleCP(restore)", error));
                    }
                }
            }
            if self.output_mode_changed {
                match unsafe { SetConsoleMode(self.output_handle, CONSOLE_MODE(self.output_mode)) }
                {
                    Ok(()) => self.output_mode_changed = false,
                    Err(error) => {
                        first_error.get_or_insert_with(|| {
                            api_error("SetConsoleMode(stdout restore)", error)
                        });
                    }
                }
            }
            if self.input_mode_changed {
                match unsafe { SetConsoleMode(self.input_handle, CONSOLE_MODE(self.input_mode)) } {
                    Ok(()) => self.input_mode_changed = false,
                    Err(error) => {
                        first_error.get_or_insert_with(|| {
                            api_error("SetConsoleMode(stdin restore)", error)
                        });
                    }
                }
            }

            first_error.map_or(Ok(()), Err)
        }
    }

    impl Drop for WindowsConsoleGuard {
        fn drop(&mut self) {
            // Drop cannot return an error. Call restore() to observe failures;
            // on implicit drop, all settings are still attempted best-effort.
            let _ = self.restore();
        }
    }
}

#[cfg(windows)]
pub use platform::WindowsConsoleGuard;

#[cfg(not(windows))]
/// No-op RAII guard on platforms without the Windows console APIs.
#[must_use = "the guard is a no-op on this platform"]
pub struct WindowsConsoleGuard;

#[cfg(not(windows))]
impl WindowsConsoleGuard {
    /// Construct the platform-compatible no-op guard.
    pub fn new() -> Result<Self, WindowsConsoleError> {
        Ok(Self)
    }

    /// Restore the original settings (a no-op on non-Windows platforms).
    pub fn restore(&mut self) -> Result<(), WindowsConsoleError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{enable_vt_input, enable_vt_output};

    #[test]
    fn vt_mode_helpers_preserve_existing_bits() {
        assert_eq!(enable_vt_input(0x0001), 0x0201);
        assert_eq!(enable_vt_output(0x0001), 0x000d);
        assert_eq!(enable_vt_input(0x0201), 0x0201);
        assert_eq!(enable_vt_output(0x000d), 0x000d);
    }
}

#[cfg(all(test, not(windows)))]
mod non_windows_tests {
    use super::WindowsConsoleGuard;

    #[test]
    fn guard_is_a_compatible_no_op() {
        let mut guard = WindowsConsoleGuard::new().expect("no-op guard construction succeeds");
        guard.restore().expect("no-op restore succeeds");
        drop(guard);
    }
}
