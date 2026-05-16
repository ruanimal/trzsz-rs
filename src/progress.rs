/*
MIT License

Copyright (c) 2022-2026 The Trzsz Authors.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, without limitation the rights
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

use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use unicode_width::UnicodeWidthStr;

pub trait ProgressCallback {
    fn on_num(&mut self, num: i64);
    fn on_name(&mut self, name: &str);
    fn on_size(&mut self, size: i64);
    fn on_step(&mut self, step: i64);
    fn on_done(&mut self);
    fn set_pre_size(&mut self, size: i64);
    fn set_pause(&mut self, pausing: bool);
}

// ─── Size / time formatting ────────────────────────────────────────────────

pub fn convert_size_to_string(size: f64) -> String {
    let mut size = size;
    let mut unit = "B";
    if size >= 1024.0 {
        size /= 1024.0;
        unit = "KB";
        if size >= 1024.0 {
            size /= 1024.0;
            unit = "MB";
            if size >= 1024.0 {
                size /= 1024.0;
                unit = "GB";
                if size >= 1024.0 {
                    size /= 1024.0;
                    unit = "TB";
                }
            }
        }
    }
    if size >= 100.0 {
        format!("{:.0} {}", size, unit)
    } else if size >= 10.0 {
        format!("{:.1} {}", size, unit)
    } else {
        format!("{:.2} {}", size, unit)
    }
}

pub fn convert_time_to_string(seconds: f64) -> String {
    let mut result = String::new();
    let mut seconds = seconds;
    if seconds >= 3600.0 {
        let hour = (seconds / 3600.0).floor();
        result.push_str(&format!("{:.0}:", hour));
        seconds -= hour * 3600.0;
    }
    let minute = (seconds / 60.0).floor();
    if minute >= 10.0 {
        result.push_str(&format!("{:.0}:", minute));
    } else {
        result.push_str(&format!("0{:.0}:", minute));
    }
    let second = seconds - (minute * 60.0);
    if second >= 10.0 {
        result.push_str(&format!("{:.0}", second));
    } else {
        result.push_str(&format!("0{:.0}", second));
    }
    result
}

fn get_ellipsis_string(s: &str, max: usize) -> (String, usize) {
    let max_inner = max - 3;
    let mut width = 0;
    let mut result = String::new();
    for ch in s.chars() {
        let w = unicode_width::UnicodeWidthChar::width(ch).unwrap_or(0);
        if width + w > max_inner {
            result.push_str("...");
            return (result, width + 3);
        }
        width += w;
        result.push(ch);
    }
    result.push_str("...");
    (result, width + 3)
}

// ─── Recent speed tracker ──────────────────────────────────────────────────

const SPEED_ARRAY_SIZE: usize = 30;

struct RecentSpeed {
    speed_cnt: usize,
    speed_idx: usize,
    time_array: [Option<Instant>; SPEED_ARRAY_SIZE],
    step_array: [i64; SPEED_ARRAY_SIZE],
}

impl RecentSpeed {
    fn new() -> Self {
        RecentSpeed {
            speed_cnt: 0,
            speed_idx: 0,
            time_array: [None; SPEED_ARRAY_SIZE],
            step_array: [0; SPEED_ARRAY_SIZE],
        }
    }

    fn init_first_step(&mut self, now: Instant) {
        self.time_array[0] = Some(now);
        self.step_array[0] = 0;
        self.speed_cnt = 1;
        self.speed_idx = 1;
    }

    fn get_speed(&mut self, step: i64, now: Instant) -> f64 {
        let speed = if self.speed_cnt <= SPEED_ARRAY_SIZE {
            self.speed_cnt += 1;
            let base_time = self.time_array[0].unwrap();
            let base_step = self.step_array[0];
            let elapsed = now.duration_since(base_time).as_secs_f64();
            if elapsed > 0.0 {
                (step - base_step) as f64 / elapsed
            } else {
                -1.0
            }
        } else {
            let idx = self.speed_idx;
            let base_time = self.time_array[idx].unwrap();
            let base_step = self.step_array[idx];
            let elapsed = now.duration_since(base_time).as_secs_f64();
            if elapsed > 0.0 {
                (step - base_step) as f64 / elapsed
            } else {
                -1.0
            }
        };

        self.time_array[self.speed_idx] = Some(now);
        self.step_array[self.speed_idx] = step;
        self.speed_idx += 1;
        if self.speed_idx >= SPEED_ARRAY_SIZE {
            self.speed_idx = 0;
        }

        if speed.is_nan() {
            -1.0
        } else {
            speed
        }
    }
}

// ─── Text progress bar ─────────────────────────────────────────────────────

pub struct TextProgressBar {
    writer: Arc<Mutex<dyn Write + Send>>,
    pub columns: AtomicI32,
    tmux_pane_columns: AtomicI32,
    file_count: i32,
    file_idx: i32,
    file_name: String,
    pre_size: i64,
    file_size: i64,
    file_step: i64,
    start_time: Option<Instant>,
    last_update_time: Option<Instant>,
    first_write: bool,
    recent_speed: RecentSpeed,
    pausing: AtomicBool,
    tmux_prefix: String,
}

impl TextProgressBar {
    pub fn new(writer: Arc<Mutex<dyn Write + Send>>, columns: i32, tmux_pane_columns: i32, tmux_prefix: &str) -> Self {
        let effective_columns = if tmux_pane_columns > 1 {
            tmux_pane_columns - 1
        } else {
            columns
        };
        TextProgressBar {
            writer,
            columns: AtomicI32::new(effective_columns),
            tmux_pane_columns: AtomicI32::new(tmux_pane_columns),
            file_count: 0,
            file_idx: 0,
            file_name: String::new(),
            pre_size: 0,
            file_size: 0,
            file_step: -1,
            start_time: None,
            last_update_time: None,
            first_write: true,
            recent_speed: RecentSpeed::new(),
            pausing: AtomicBool::new(false),
            tmux_prefix: tmux_prefix.to_string(),
        }
    }

    fn hide_cursor(&self) {
        self.write_progress("\x1b[?25l");
    }

    pub fn show_cursor(&self) {
        self.write_progress("\x1b[?25h");
    }

    fn write_progress(&self, progress: &str) {
        if let Ok(mut writer) = self.writer.lock() {
            if !self.tmux_prefix.is_empty() {
                let data = progress.as_bytes();
                let mut encoded = Vec::with_capacity(self.tmux_prefix.len() + data.len() * 4 + 2);
                encoded.extend_from_slice(self.tmux_prefix.as_bytes());
                for &b in data {
                    if b < b' ' || b == b'\\' || b > b'~' {
                        encoded.extend_from_slice(format!("\\{:03o}", b).as_bytes());
                    } else {
                        encoded.push(b);
                    }
                }
                encoded.extend_from_slice(b"\r\n");
                let _ = writer.write_all(&encoded);
            } else {
                let _ = writer.write_all(progress.as_bytes());
            }
        }
    }

    fn show_progress(&mut self) {
        let now = Instant::now();
        if let Some(last) = self.last_update_time {
            if now.duration_since(last) < Duration::from_millis(200) {
                return;
            }
        }
        self.last_update_time = Some(now);

        let percentage = if self.file_size == 0 {
            "100%".to_string()
        } else {
            format!("{:.0}%", (self.file_step as f64 * 100.0 / self.file_size as f64).round())
        };
        let total = convert_size_to_string(self.file_step as f64);
        let speed = self.recent_speed.get_speed(self.file_step, now);
        let speed_str = if speed > 0.0 {
            format!("{}/s", convert_size_to_string(speed))
        } else {
            "--- B/s".to_string()
        };
        let eta_str = if speed > 0.0 {
            let remaining = (self.file_size - self.file_step) as f64 / speed;
            format!("{} ETA", convert_time_to_string(remaining.round()))
        } else {
            "--- ETA".to_string()
        };

        let progress_text = self.get_progress_text(&percentage, &total, &speed_str, &eta_str);

        if self.first_write {
            self.first_write = false;
            self.write_progress(&format!("\x1b[?7l{}\x1b[?7h", progress_text));
            return;
        }

        if self.tmux_pane_columns.load(Ordering::Relaxed) > 0 {
            let cols = self.columns.load(Ordering::Relaxed);
            self.write_progress(&format!("\x1b[{}D\x1b[?7l{}\x1b[?7h", cols, progress_text));
        } else {
            self.write_progress(&format!("\r\x1b[?7l{}\x1b[?7h", progress_text));
        }
    }

    fn get_progress_text(&self, percentage: &str, total: &str, speed: &str, eta: &str) -> String {
        const BAR_MIN_LENGTH: usize = 24;

        let mut left = if self.file_count > 1 {
            format!("({}/{}) {}", self.file_idx, self.file_count, self.file_name)
        } else {
            self.file_name.clone()
        };
        let mut left_length = UnicodeWidthStr::width(left.as_str());
        let mut right = format!(" {} | {} | {} | {}", percentage, total, speed, eta);

        // Try to fit within columns
        let cols = self.columns.load(Ordering::Relaxed) as usize;
        if cols.saturating_sub(left_length).saturating_sub(right.len()) < BAR_MIN_LENGTH {
            if left_length > 50 {
                let (s, l) = get_ellipsis_string(&left, 50);
                left = s;
                left_length = l;
            }
            if cols.saturating_sub(left_length).saturating_sub(right.len()) < BAR_MIN_LENGTH {
                if left_length > 40 {
                    let (s, l) = get_ellipsis_string(&left, 40);
                    left = s;
                    left_length = l;
                }
                if cols.saturating_sub(left_length).saturating_sub(right.len()) < BAR_MIN_LENGTH {
                    right = format!(" {} | {} | {}", percentage, speed, eta);
                    if cols.saturating_sub(left_length).saturating_sub(right.len()) < BAR_MIN_LENGTH {
                        if left_length > 30 {
                            let (s, l) = get_ellipsis_string(&left, 30);
                            left = s;
                            left_length = l;
                        }
                        if cols.saturating_sub(left_length).saturating_sub(right.len()) < BAR_MIN_LENGTH {
                            right = format!(" {} | {}", percentage, eta);
                            if cols.saturating_sub(left_length).saturating_sub(right.len()) < BAR_MIN_LENGTH {
                                right = format!(" {}", percentage);
                                if cols.saturating_sub(left_length).saturating_sub(right.len()) < BAR_MIN_LENGTH {
                                    if left_length > 20 {
                                        let (s, l) = get_ellipsis_string(&left, 20);
                                        left = s;
                                        left_length = l;
                                    }
                                    if cols.saturating_sub(left_length).saturating_sub(right.len()) < BAR_MIN_LENGTH {
                                        left.clear();
                                        left_length = 0;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let bar_length = cols.saturating_sub(right.len()).saturating_sub(if left_length > 0 { left_length + 1 } else { 0 });
        let bar = self.get_progress_bar(bar_length);

        let result = if left_length > 0 {
            format!("{} {} {}", left, bar, right)
        } else {
            format!("{} {}", bar, right)
        };
        result.trim().to_string()
    }

    fn get_progress_bar(&self, bar_length: usize) -> String {
        if bar_length < 2 {
            return String::new();
        }
        let filled = if self.file_size == 0 {
            bar_length
        } else {
            (self.file_step as f64 * bar_length as f64 / self.file_size as f64).round() as usize
        };
        let filled = filled.min(bar_length);
        let empty = bar_length.saturating_sub(filled);
        format!("[{}{}]", "#".repeat(filled), "-".repeat(empty))
    }
}

impl ProgressCallback for TextProgressBar {
    fn on_num(&mut self, num: i64) {
        self.file_count = num as i32;
        self.hide_cursor();
    }

    fn on_name(&mut self, name: &str) {
        self.file_name = name.to_string();
        self.file_idx += 1;
        let now = Instant::now();
        self.start_time = Some(now);
        self.recent_speed.init_first_step(now);
        self.pre_size = 0;
        self.file_step = -1;
    }

    fn on_size(&mut self, size: i64) {
        self.file_size = self.pre_size + size;
    }

    fn on_step(&mut self, step: i64) {
        let step = step + self.pre_size;
        if step <= self.file_step {
            return;
        }
        self.file_step = step;
        if !self.pausing.load(Ordering::Relaxed) {
            self.show_progress();
        }
    }

    fn on_done(&mut self) {
        if self.file_size == 0 {
            return;
        }
        self.file_step = self.file_size;
        self.last_update_time = None;
        self.show_progress();
    }

    fn set_pre_size(&mut self, size: i64) {
        self.pre_size = size;
    }

    fn set_pause(&mut self, pausing: bool) {
        if !pausing {
            self.hide_cursor();
        }
        self.pausing.store(pausing, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_size_to_string() {
        assert_eq!(convert_size_to_string(0.0), "0.00 B");
        assert_eq!(convert_size_to_string(512.0), "512 B");
        assert_eq!(convert_size_to_string(1024.0), "1.00 KB");
        assert_eq!(convert_size_to_string(10240.0), "10.0 KB");
        assert_eq!(convert_size_to_string(102400.0), "100 KB");
        assert_eq!(convert_size_to_string(1048576.0), "1.00 MB");
        assert_eq!(convert_size_to_string(1073741824.0), "1.00 GB");
    }

    #[test]
    fn test_convert_time_to_string() {
        assert_eq!(convert_time_to_string(0.0), "00:00");
        assert_eq!(convert_time_to_string(5.0), "00:05");
        assert_eq!(convert_time_to_string(65.0), "01:05");
        assert_eq!(convert_time_to_string(3665.0), "1:01:05");
    }

    #[test]
    fn test_get_ellipsis_string() {
        let (s, w) = get_ellipsis_string("hello world", 8);
        assert_eq!(s, "hello...");
        assert_eq!(w, 8);
    }
}
