/*
MIT License

Copyright (c) 2022-2026 The Trzsz Authors.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

use std::io::{self, Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::SyncSender;

use crate::transfer::TrzszTransfer;

const MAX_PROTOCOL_HEADER_SIZE: usize = 128;
const MAX_DATA_FRAME_SIZE: u64 = 1024 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct StopPromptController {
    pausing: Arc<AtomicBool>,
    pause_idx: Arc<AtomicU32>,
    pause_supported: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    stop_and_delete: Arc<AtomicBool>,
    menu_active: Arc<AtomicBool>,
}

impl StopPromptController {
    pub(crate) fn new(transfer: &TrzszTransfer) -> Self {
        let (pausing, pause_idx, pause_supported) = transfer.pause_handles();
        StopPromptController {
            pausing,
            pause_idx,
            pause_supported,
            stopped: transfer.stop_handle(),
            stop_and_delete: transfer.stop_and_delete_handle(),
            menu_active: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn handle_interrupt(&self) {
        self.handle_interrupt_with(show_stop_menu);
    }

    pub(crate) fn handle_filter_input(&self, key: u8, output: &mut dyn Write) {
        if self.is_menu_active() {
            self.handle_menu_key(key);
        } else if key == 0x03 {
            self.handle_interrupt_with(|| show_stop_menu_to(output));
        }
    }

    pub(crate) fn handle_filter_eof(&self) {
        if self.is_menu_active() {
            self.finish(StopChoice::Continue);
        } else if self.pausing.load(Ordering::SeqCst) {
            self.finish(StopChoice::KeepFiles);
        }
    }

    fn handle_interrupt_with(&self, show_menu: impl FnOnce()) {
        if !self.pause_supported.load(Ordering::SeqCst) {
            self.finish(StopChoice::KeepFiles);
            return;
        }
        if self.menu_active.load(Ordering::SeqCst) {
            self.finish(StopChoice::KeepFiles);
            return;
        }
        if !self.pausing.swap(true, Ordering::SeqCst) {
            self.pause_idx.fetch_add(1, Ordering::SeqCst);
            return;
        }
        if self
            .menu_active
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            show_menu();
        } else {
            self.finish(StopChoice::KeepFiles);
        }
    }
    fn is_menu_active(&self) -> bool {
        self.menu_active.load(Ordering::SeqCst)
    }

    fn handle_menu_key(&self, key: u8) -> bool {
        if !self.is_menu_active() {
            return false;
        }
        let choice = match key {
            b'1' | 0x03 => Some(StopChoice::KeepFiles),
            b'2' => Some(StopChoice::DeleteFiles),
            b'3' | b'q' | b'Q' | 0x11 => Some(StopChoice::Continue),
            _ => None,
        };
        if let Some(choice) = choice {
            self.finish(choice);
            true
        } else {
            false
        }
    }

    fn finish(&self, choice: StopChoice) {
        self.menu_active.store(false, Ordering::SeqCst);
        self.pausing.store(false, Ordering::SeqCst);
        match choice {
            StopChoice::Continue => {}
            StopChoice::KeepFiles => self.stop(false),
            StopChoice::DeleteFiles => self.stop(true),
        }
    }

    pub(crate) fn stop(&self, delete: bool) {
        self.stop_and_delete.store(delete, Ordering::SeqCst);
        self.stopped.store(true, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StopChoice {
    KeepFiles,
    DeleteFiles,
    Continue,
}

fn show_stop_menu() {
    let mut stderr = io::stderr().lock();
    show_stop_menu_to(&mut stderr);
}

fn show_stop_menu_to(output: &mut dyn Write) {
    let _ = output.write_all(
        b"\r\nAre you sure you want to stop transferring files?\r\n\
[1] Stop and keep transferred files\r\n\
[2] Stop and delete transferred files\r\n\
[3] Continue to transfer remaining files (q to continue)\r\n",
    );
    let _ = output.flush();
}

pub(crate) fn run_stdin_reader<R: Read>(
    mut reader: R,
    sender: SyncSender<Vec<u8>>,
    controller: StopPromptController,
) {
    let mut protocol = ProtocolInputTracker::default();
    let mut input = [0u8; 32 * 1024];
    loop {
        let count = match reader.read(&mut input) {
            Ok(0) => {
                if controller.is_menu_active() {
                    controller.finish(StopChoice::Continue);
                } else if controller.pausing.load(Ordering::SeqCst) {
                    controller.finish(StopChoice::KeepFiles);
                }
                return;
            }
            Ok(count) => count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => return,
        };
        let mut to_transfer = Vec::with_capacity(count);
        for &byte in &input[..count] {
            // A framed DATA payload may contain ETX as file data; without the client filter input pump it cannot be distinguished from a keyboard Ctrl+C.
            if byte == 0x03 && !protocol.is_data_payload_byte() {
                if controller.is_menu_active() {
                    controller.handle_menu_key(byte);
                } else {
                    if !send_buffer(&sender, &mut to_transfer) {
                        return;
                    }
                    controller.handle_interrupt();
                }
                continue;
            }
            if protocol.is_protocol_byte(byte) {
                to_transfer.push(byte);
            } else if controller.is_menu_active() {
                controller.handle_menu_key(byte);
            } else {
                to_transfer.push(byte);
            }
        }
        if !send_buffer(&sender, &mut to_transfer) {
            return;
        }
    }
}

fn send_buffer(sender: &SyncSender<Vec<u8>>, buffer: &mut Vec<u8>) -> bool {
    if buffer.is_empty() {
        return true;
    }
    sender.send(std::mem::take(buffer)).is_ok()
}

#[derive(Default)]
struct ProtocolInputTracker {
    in_header: bool,
    header: Vec<u8>,
    data_remaining: u64,
}

impl ProtocolInputTracker {
    fn is_data_payload_byte(&self) -> bool {
        self.data_remaining > 0
    }

    fn is_protocol_byte(&mut self, byte: u8) -> bool {
        if self.data_remaining > 0 {
            self.data_remaining -= 1;
            return true;
        }
        if self.in_header {
            if self.header.len() < MAX_PROTOCOL_HEADER_SIZE {
                self.header.push(byte);
            }
            if byte == b'\n' {
                self.in_header = false;
                self.data_remaining = data_frame_size(&self.header).unwrap_or(0);
                self.header.clear();
            }
            return true;
        }
        if byte == b'#' {
            self.in_header = true;
            self.header.clear();
            self.header.push(byte);
            return true;
        }
        false
    }
}

fn data_frame_size(header: &[u8]) -> Option<u64> {
    let header = header.strip_prefix(b"#DATA:")?;
    let header = header.strip_suffix(b"\n").unwrap_or(header);
    let header = header.strip_suffix(b"\r").unwrap_or(header);
    let size = std::str::from_utf8(header).ok()?.parse::<u64>().ok()?;
    (size <= MAX_DATA_FRAME_SIZE).then_some(size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::mpsc;

    fn controller() -> (TrzszTransfer, StopPromptController) {
        let transfer = TrzszTransfer::new(Box::new(io::sink()));
        let (_, _, pause_supported) = transfer.pause_handles();
        pause_supported.store(true, Ordering::SeqCst);
        let controller = StopPromptController::new(&transfer);
        (transfer, controller)
    }

    #[test]
    fn paused_menu_can_keep_delete_or_continue() {
        for (key, expected_delete, expected_stopped) in [
            (b'1', false, true),
            (b'2', true, true),
            (b'3', false, false),
        ] {
            let (transfer, controller) = controller();
            controller.handle_interrupt();
            assert!(controller.pausing.load(Ordering::SeqCst));
            controller.handle_interrupt();
            assert!(controller.is_menu_active());
            assert!(controller.handle_menu_key(key));
            assert_eq!(
                controller.stop_and_delete.load(Ordering::SeqCst),
                expected_delete
            );
            assert_eq!(transfer.buffer.is_stopped(), expected_stopped);
            assert!(!controller.pausing.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn stdin_interrupt_menu_preserves_protocol_payload_and_deletes_on_choice() {
        let (mut transfer, controller) = controller();
        let directory = tempfile::tempdir().unwrap();
        let created_file = directory.path().join("partial.bin");
        std::fs::write(&created_file, b"partial").unwrap();
        transfer.add_created_files(created_file.to_str().unwrap());

        let (sender, receiver) = mpsc::sync_channel(8);
        let wire = b"#DATA:3\n\x0312\x03\x032";
        run_stdin_reader(Cursor::new(wire), sender, controller.clone());

        assert_eq!(receiver.try_recv().unwrap(), b"#DATA:3\n\x0312");
        assert!(controller.stop_and_delete.load(Ordering::SeqCst));
        assert!(transfer.buffer.is_stopped());
        let error = transfer.check_stop().unwrap_err();
        assert_eq!(error.message, "Stopped and deleted");
        transfer.client_error(&error);
        assert!(!created_file.exists());
    }

    #[test]
    fn protocol_data_payload_is_not_treated_as_a_ctrl_c_key() {
        let mut tracker = ProtocolInputTracker::default();
        for &byte in b"#DATA:3\n\x0312" {
            assert!(tracker.is_protocol_byte(byte));
        }
        assert!(!tracker.is_protocol_byte(0x03));
    }
    #[test]
    fn v1_v2_interrupt_stops_without_showing_menu() {
        let transfer = TrzszTransfer::new(Box::new(io::sink()));
        let controller = StopPromptController::new(&transfer);
        let sender = transfer.buffer.sender();

        run_stdin_reader(Cursor::new([0x03]), sender, controller.clone());

        assert!(!controller.is_menu_active());
        assert!(transfer.buffer.is_stopped());
        assert_eq!(transfer.check_stop().unwrap_err().message, "Stopped");
    }
    #[test]
    fn stdin_eof_while_paused_stops_instead_of_stalling() {
        let (transfer, controller) = controller();
        let sender = transfer.buffer.sender();

        run_stdin_reader(Cursor::new([0x03]), sender, controller.clone());

        assert!(!controller.pausing.load(Ordering::SeqCst));
        assert!(transfer.buffer.is_stopped());
        assert_eq!(transfer.check_stop().unwrap_err().message, "Stopped");
    }
}
