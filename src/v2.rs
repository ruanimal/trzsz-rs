use std::cell::Cell;
use std::collections::VecDeque;
use std::io::{self, Read, Write};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::read::DecoderReader;
use base64::write::EncoderWriter;
use md5::Md5;
use sha2::Digest;

use crate::comm::{CompressType, FileReader, FileWriter, TrzszError};
use crate::escape::{self, EscapeTable};
use crate::progress::ProgressCallback;
use crate::transfer::TrzszTransfer;

const ACK_WINDOW: usize = 5;
const MAX_FRAME_SIZE: usize = 32 * 1024;
const COMPRESSION_SAMPLE_SIZE: usize = 128 * 1024;
const MAX_ZSTD_WINDOW_LOG: u32 = 27;
const MAX_V2_WIRE_FRAME_SIZE: usize = 64 * 1024 * 1024;

pub(crate) fn send_file_data(
    transfer: &mut TrzszTransfer,
    file: &mut dyn FileReader,
    progress: &mut Option<&mut dyn ProgressCallback>,
) -> Result<Vec<u8>, TrzszError> {
    let size = file.size();
    if size < 0 {
        return Err(crate::comm::simple_error("Invalid file size"));
    }
    let rust_peer = is_rust_peer(transfer);
    let compress = if transfer.transfer_config.protocol >= 3 {
        match v3_fixed_compression(transfer, size) {
            Some(compress) => compress,
            None => {
                let compress = auto_compress(file, size)?;
                transfer.send_line("COMP", if compress { "true" } else { "false" })?;
                compress
            }
        }
    } else if rust_peer {
        let compress = match transfer.transfer_config.compress {
            x if x == CompressType::Yes as i32 => true,
            x if x == CompressType::No as i32 => false,
            _ => auto_compress(file, size)?,
        };
        transfer.send_line("COMP", if compress { "true" } else { "false" })?;
        compress
    } else {
        // Go protocol 2 fixes compression to !binary and does not exchange COMP.
        !transfer.transfer_config.binary
    };
    let binary = transfer.transfer_config.binary;
    let table = get_escape_table(transfer)?;
    let digest = if binary {
        let frame_writer = FrameWriter::new(transfer, true, size, progress);
        if compress {
            let escaped = EscapeWriter {
                inner: frame_writer,
                table,
            };
            let mut encoder = zstd::stream::write::Encoder::new(escaped, 0)
                .map_err(|e| crate::comm::simple_trzsz_error("Create zstd encoder failed", e))?;
            let digest = read_file_to_writer(file, size, &mut encoder)?;
            let escaped = encoder
                .finish()
                .map_err(|e| crate::comm::simple_trzsz_error("Finish zstd encoder failed", e))?;
            escaped.inner.finish()?;
            digest
        } else {
            let mut escaped = EscapeWriter {
                inner: frame_writer,
                table,
            };
            let digest = read_file_to_writer(file, size, &mut escaped)?;
            escaped.inner.finish()?;
            digest
        }
    } else {
        let frame_writer = FrameWriter::new(transfer, false, size, progress);
        let base64_writer = EncoderWriter::new(frame_writer, &BASE64);
        if compress {
            let mut encoder = zstd::stream::write::Encoder::new(base64_writer, 0)
                .map_err(|e| crate::comm::simple_trzsz_error("Create zstd encoder failed", e))?;
            let digest = read_file_to_writer(file, size, &mut encoder)?;
            let mut base64_writer = encoder
                .finish()
                .map_err(|e| crate::comm::simple_trzsz_error("Finish zstd encoder failed", e))?;
            let frame_writer = base64_writer
                .finish()
                .map_err(|e| crate::comm::simple_trzsz_error("Finish base64 encoder failed", e))?;
            frame_writer.finish()?;
            digest
        } else {
            let mut base64_writer = base64_writer;
            let digest = read_file_to_writer(file, size, &mut base64_writer)?;
            let frame_writer = base64_writer
                .finish()
                .map_err(|e| crate::comm::simple_trzsz_error("Finish base64 encoder failed", e))?;
            frame_writer.finish()?;
            digest
        }
    };
    Ok(digest)
}

pub(crate) fn recv_file_data(
    transfer: &mut TrzszTransfer,
    file: &mut dyn FileWriter,
    size: i64,
    progress: &mut Option<&mut dyn ProgressCallback>,
) -> Result<Vec<u8>, TrzszError> {
    if size < 0 {
        return Err(crate::comm::simple_error("Invalid file size"));
    }
    let compress = if transfer.transfer_config.protocol >= 3 {
        match v3_fixed_compression(transfer, size) {
            Some(compress) => compress,
            None => recv_compress_flag(transfer)?,
        }
    } else if is_rust_peer(transfer) {
        recv_compress_flag(transfer)?
    } else {
        !transfer.transfer_config.binary
    };
    let binary = transfer.transfer_config.binary;
    let table = get_escape_table(transfer)?;
    let saved = Cell::new(0_i64);
    let ended = Cell::new(false);

    let digest = if compress {
        if binary {
            let frame_reader = FrameReader::new(transfer, true, table, &saved, &ended);
            let mut decoder = zstd::stream::read::Decoder::new(frame_reader)
                .map_err(|e| crate::comm::simple_trzsz_error("Create zstd decoder failed", e))?;
            decoder
                .window_log_max(MAX_ZSTD_WINDOW_LOG)
                .map_err(|e| crate::comm::simple_trzsz_error("Set zstd window limit failed", e))?;
            let digest = save_reader(&mut decoder, file, size, &saved, progress)?;
            if !ended.get() {
                return Err(crate::comm::simple_error(
                    "Compressed DATA stream ended without finish marker",
                ));
            }
            digest
        } else {
            let frame_reader = FrameReader::new(transfer, false, table, &saved, &ended);
            let base64_reader = DecoderReader::new(frame_reader, &BASE64);
            let mut decoder = zstd::stream::read::Decoder::new(base64_reader)
                .map_err(|e| crate::comm::simple_trzsz_error("Create zstd decoder failed", e))?;
            decoder
                .window_log_max(MAX_ZSTD_WINDOW_LOG)
                .map_err(|e| crate::comm::simple_trzsz_error("Set zstd window limit failed", e))?;
            let digest = save_reader(&mut decoder, file, size, &saved, progress)?;
            if !ended.get() {
                return Err(crate::comm::simple_error(
                    "Compressed DATA stream ended without finish marker",
                ));
            }
            digest
        }
    } else if binary {
        let mut reader = FrameReader::new(transfer, true, table, &saved, &ended);
        let digest = save_reader(&mut reader, file, size, &saved, progress)?;
        if !ended.get() {
            return Err(crate::comm::simple_error(
                "DATA stream ended without finish marker",
            ));
        }
        digest
    } else {
        let frame_reader = FrameReader::new(transfer, false, table, &saved, &ended);
        let mut decoder = DecoderReader::new(frame_reader, &BASE64);
        let digest = save_reader(&mut decoder, file, size, &saved, progress)?;
        if !ended.get() {
            return Err(crate::comm::simple_error(
                "DATA stream ended without finish marker",
            ));
        }
        digest
    };

    transfer.send_integer("SUCC", saved.get())?;
    Ok(digest)
}

fn v3_fixed_compression(transfer: &TrzszTransfer, size: i64) -> Option<bool> {
    match transfer.transfer_config.compress {
        x if x == CompressType::Yes as i32 => Some(true),
        x if x == CompressType::No as i32 => Some(false),
        _ if size < 512 => Some(false),
        _ if size < COMPRESSION_SAMPLE_SIZE as i64 => Some(true),
        _ => None,
    }
}

fn recv_compress_flag(transfer: &mut TrzszTransfer) -> Result<bool, TrzszError> {
    match transfer
        .recv_check("COMP", false, transfer.get_new_timeout())?
        .as_str()
    {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(crate::comm::simple_trzsz_error(
            "Unknown compress flag",
            other,
        )),
    }
}
fn is_rust_peer(transfer: &TrzszTransfer) -> bool {
    transfer.transfer_config.protocol >= 2
        && transfer.transfer_config.lang == "rust"
        && transfer.peer_lang == "rust"
}

fn get_escape_table(transfer: &TrzszTransfer) -> Result<EscapeTable, TrzszError> {
    if let Some(chars) = &transfer.transfer_config.escape_chars {
        let arr = chars
            .as_array()
            .ok_or_else(|| crate::comm::simple_error("Escape chars invalid"))?;
        escape::escape_chars_to_table(arr)
    } else {
        Ok(EscapeTable::default())
    }
}

fn read_file_to_writer(
    file: &mut dyn FileReader,
    size: i64,
    writer: &mut dyn Write,
) -> Result<Vec<u8>, TrzszError> {
    let mut buffer = vec![0_u8; MAX_FRAME_SIZE];
    let mut step = 0_i64;
    let mut hasher = Md5::new();
    while step < size {
        let want = usize::try_from((size - step).min(buffer.len() as i64))
            .map_err(|e| crate::comm::simple_trzsz_error("Invalid file size", e))?;
        let count = file.read(&mut buffer[..want]).map_err(|e| TrzszError {
            message: e.to_string(),
            err_type: String::new(),
            trace: false,
        })?;
        if count == 0 {
            return Err(crate::comm::simple_trzsz_error(
                "Unexpected EOF",
                format!("sent {} of {} bytes", step, size),
            ));
        }
        writer.write_all(&buffer[..count]).map_err(|e| TrzszError {
            message: e.to_string(),
            err_type: String::new(),
            trace: false,
        })?;
        hasher.update(&buffer[..count]);
        step += count as i64;
    }
    Ok(hasher.finalize().to_vec())
}

fn auto_compress(file: &mut dyn FileReader, size: i64) -> Result<bool, TrzszError> {
    if size < 512 {
        return Ok(false);
    }
    if size < COMPRESSION_SAMPLE_SIZE as i64 {
        return Ok(true);
    }

    let origin = match file.seek(io::SeekFrom::Current(0)) {
        Ok(pos) => pos,
        Err(_) => return Ok(true),
    };
    let result = (|| {
        let block = COMPRESSION_SAMPLE_SIZE as i64;
        let mut positions = vec![0_i64];
        if size >= 2 * block {
            positions.push(size - block);
        }
        if size >= 3 * block {
            positions.push(size / 2 - block / 2);
        }
        let mut compressed_count = 0;
        let mut sample = vec![0_u8; COMPRESSION_SAMPLE_SIZE];
        for position in positions {
            let offset = origin
                .checked_add(u64::try_from(position).map_err(|e| {
                    crate::comm::simple_trzsz_error("Invalid compression sample position", e)
                })?)
                .ok_or_else(|| crate::comm::simple_error("Invalid compression sample position"))?;
            file.seek(io::SeekFrom::Start(offset)).map_err(|e| {
                crate::comm::simple_trzsz_error("Compression sample seek failed", e)
            })?;
            let mut read = 0;
            while read < sample.len() {
                let count = file.read(&mut sample[read..]).map_err(|e| TrzszError {
                    message: e.to_string(),
                    err_type: String::new(),
                    trace: false,
                })?;
                if count == 0 {
                    return Err(crate::comm::simple_error(
                        "Unexpected EOF while checking compression",
                    ));
                }
                read += count;
            }
            let compressed = zstd::bulk::compress(&sample, 0)
                .map_err(|e| crate::comm::simple_trzsz_error("Compression probe failed", e))?;
            if compressed.len() > sample.len() * 98 / 100 {
                compressed_count += 1;
            }
        }
        Ok(compressed_count < 2)
    })();
    let restore = file.seek(io::SeekFrom::Start(origin));
    match (result, restore) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(crate::comm::simple_trzsz_error(
            "Compression probe restore failed",
            error,
        )),
        (Ok(compress), Ok(_)) => Ok(compress),
    }
}

struct FrameWriter<'t, 'p, 'c> {
    transfer: &'t mut TrzszTransfer,
    binary: bool,
    file_size: i64,
    buffer: Vec<u8>,
    frame_size: usize,
    pending: VecDeque<usize>,
    last_step: i64,
    progress: &'p mut Option<&'c mut dyn ProgressCallback>,
}

impl<'t, 'p, 'c> FrameWriter<'t, 'p, 'c> {
    fn new(
        transfer: &'t mut TrzszTransfer,
        binary: bool,
        file_size: i64,
        progress: &'p mut Option<&'c mut dyn ProgressCallback>,
    ) -> Self {
        let frame_size = usize::try_from(transfer.transfer_config.bufsize.max(1024))
            .unwrap_or(MAX_FRAME_SIZE)
            .min(MAX_FRAME_SIZE);
        FrameWriter {
            transfer,
            binary,
            file_size,
            buffer: Vec::with_capacity(frame_size),
            frame_size,
            pending: VecDeque::new(),
            last_step: 0,
            progress,
        }
    }

    fn send_payload(&mut self, data: &[u8]) -> Result<(), TrzszError> {
        while self.pending.len() >= ACK_WINDOW {
            self.recv_chunk_ack()?;
        }
        self.transfer.check_stop()?;
        let newline = self.transfer.transfer_config.newline.clone();
        if self.binary {
            self.transfer
                .write_all(format!("#DATA:{}{}", data.len(), newline).as_bytes())?;
            if !data.is_empty() {
                self.transfer.write_all(data)?;
            }
        } else {
            self.transfer.write_all(b"#DATA:")?;
            if !data.is_empty() {
                self.transfer.write_all(data)?;
            }
            self.transfer.write_all(newline.as_bytes())?;
        }
        self.pending.push_back(data.len());
        Ok(())
    }

    fn recv_chunk_ack(&mut self) -> Result<(), TrzszError> {
        let expected = self.pending.pop_front().ok_or_else(|| {
            crate::comm::simple_error("V2 ACK received without an outstanding DATA frame")
        })?;
        let response = self
            .transfer
            .recv_check("SUCC", false, self.transfer.get_new_timeout())?;
        let (length, step) = response
            .split_once('/')
            .ok_or_else(|| crate::comm::simple_error("Invalid V2 DATA acknowledgement"))?;
        let length = length
            .parse::<usize>()
            .map_err(|e| crate::comm::simple_trzsz_error("Invalid V2 DATA ACK length", e))?;
        let step = step
            .parse::<i64>()
            .map_err(|e| crate::comm::simple_trzsz_error("Invalid V2 DATA ACK step", e))?;
        if length != expected {
            return Err(crate::comm::simple_trzsz_error(
                "V2 DATA ACK length mismatch",
                format!("{} <> {}", length, expected),
            ));
        }
        self.update_step(step)
    }

    fn update_step(&mut self, step: i64) -> Result<(), TrzszError> {
        if step < self.last_step || step > self.file_size {
            return Err(crate::comm::simple_trzsz_error(
                "Invalid V2 DATA ACK step",
                format!(
                    "{} (previous {}, size {})",
                    step, self.last_step, self.file_size
                ),
            ));
        }
        self.last_step = step;
        if let Some(callback) = self.progress.as_mut() {
            callback.on_step(step);
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(), TrzszError> {
        if !self.buffer.is_empty() {
            let payload = std::mem::take(&mut self.buffer);
            self.send_payload(&payload)?;
        }
        self.send_payload(&[])?;
        while !self.pending.is_empty() {
            self.recv_chunk_ack()?;
        }
        loop {
            let step =
                self.transfer
                    .recv_integer("SUCC", false, self.transfer.get_new_timeout())?;
            self.update_step(step)?;
            if step == self.file_size {
                return Ok(());
            }
        }
    }
}

impl Write for FrameWriter<'_, '_, '_> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let mut offset = 0;
        while offset < data.len() {
            let count = (self.frame_size - self.buffer.len()).min(data.len() - offset);
            self.buffer.extend_from_slice(&data[offset..offset + count]);
            offset += count;
            if self.buffer.len() == self.frame_size {
                let payload = std::mem::take(&mut self.buffer);
                self.buffer = Vec::with_capacity(self.frame_size);
                self.send_payload(&payload).map_err(to_io_error)?;
            }
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct EscapeWriter<W> {
    inner: W,
    table: EscapeTable,
}

impl<W: Write> Write for EscapeWriter<W> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let escaped = escape::escape_data(data, &self.table);
        self.inner.write_all(&escaped)?;
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

struct FrameReader<'t, 's, 'e> {
    transfer: &'t mut TrzszTransfer,
    binary: bool,
    table: EscapeTable,
    saved: &'s Cell<i64>,
    ended: &'e Cell<bool>,
    buffer: Vec<u8>,
    offset: usize,
    escaped_tail: Vec<u8>,
}

impl<'t, 's, 'e> FrameReader<'t, 's, 'e> {
    fn new(
        transfer: &'t mut TrzszTransfer,
        binary: bool,
        table: EscapeTable,
        saved: &'s Cell<i64>,
        ended: &'e Cell<bool>,
    ) -> Self {
        FrameReader {
            transfer,
            binary,
            table,
            saved,
            ended,
            buffer: Vec::new(),
            offset: 0,
            escaped_tail: Vec::new(),
        }
    }

    fn read_next_frame(&mut self) -> io::Result<()> {
        let (raw, wire_len, finished) =
            recv_frame(self.transfer, self.binary).map_err(to_io_error)?;
        self.transfer
            .send_line("SUCC", &format!("{}/{}", wire_len, self.saved.get()))
            .map_err(to_io_error)?;
        if finished {
            if !self.escaped_tail.is_empty() {
                return Err(io::Error::other(
                    "Incomplete escape sequence in final DATA frame",
                ));
            }
            self.ended.set(true);
            return Ok(());
        }
        self.buffer.clear();
        self.offset = 0;
        if self.binary {
            let mut escaped = std::mem::take(&mut self.escaped_tail);
            escaped.extend_from_slice(&raw);
            let (decoded, remaining) =
                escape::unescape_data(&escaped, &self.table, None).map_err(to_io_error)?;
            self.buffer = decoded;
            self.escaped_tail = remaining;
        } else {
            self.buffer = raw;
        }
        Ok(())
    }
}

impl Read for FrameReader<'_, '_, '_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        while self.offset == self.buffer.len() && !self.ended.get() {
            self.read_next_frame()?;
            if self.ended.get() {
                return Ok(0);
            }
        }
        if self.offset == self.buffer.len() {
            return Ok(0);
        }
        let count = (self.buffer.len() - self.offset).min(output.len());
        output[..count].copy_from_slice(&self.buffer[self.offset..self.offset + count]);
        self.offset += count;
        Ok(count)
    }
}

fn recv_frame(
    transfer: &mut TrzszTransfer,
    binary: bool,
) -> Result<(Vec<u8>, usize, bool), TrzszError> {
    let limit = max_wire_frame_size(transfer);
    if binary {
        let size = transfer
            .recv_check_limited("DATA", false, transfer.get_new_timeout(), 64)?
            .parse::<i64>()
            .map_err(|e| crate::comm::simple_trzsz_error("Invalid DATA size", e))?;
        if size < 0 {
            return Err(crate::comm::simple_error("Invalid DATA size"));
        }
        let size = usize::try_from(size)
            .map_err(|e| crate::comm::simple_trzsz_error("Invalid DATA size", e))?;
        if size > limit {
            return Err(crate::comm::simple_error(
                "DATA frame exceeds negotiated limit",
            ));
        }
        if size == 0 {
            return Ok((Vec::new(), 0, true));
        }
        let data = transfer
            .buffer
            .read_binary(size, transfer.get_new_timeout())?;
        Ok((data, size, false))
    } else {
        let encoded = transfer.recv_check_limited(
            "DATA",
            false,
            transfer.get_new_timeout(),
            limit.saturating_add(b"#DATA:".len()),
        )?;
        if encoded.is_empty() {
            return Ok((Vec::new(), 0, true));
        }
        let data = encoded.into_bytes();
        if data.len() > limit {
            return Err(crate::comm::simple_error(
                "DATA frame exceeds negotiated limit",
            ));
        }
        let size = data.len();
        Ok((data, size, false))
    }
}

fn max_wire_frame_size(transfer: &TrzszTransfer) -> usize {
    usize::try_from(transfer.transfer_config.bufsize.max(1024))
        .unwrap_or(usize::MAX)
        .saturating_mul(2)
        .min(MAX_V2_WIRE_FRAME_SIZE)
}

fn save_reader(
    reader: &mut dyn Read,
    file: &mut dyn FileWriter,
    size: i64,
    saved: &Cell<i64>,
    progress: &mut Option<&mut dyn ProgressCallback>,
) -> Result<Vec<u8>, TrzszError> {
    let mut step = 0_i64;
    let mut hasher = Md5::new();
    let mut buffer = vec![0_u8; 32 * 1024];
    loop {
        let remaining = size - step;
        let capacity = if remaining >= buffer.len() as i64 {
            buffer.len()
        } else {
            usize::try_from(remaining.max(0) + 1).unwrap_or(1)
        };
        let count = reader
            .read(&mut buffer[..capacity])
            .map_err(|e| crate::comm::simple_trzsz_error("Read from V2 data stream failed", e))?;
        if count == 0 {
            break;
        }
        if count as i64 > size - step {
            return Err(crate::comm::simple_error(
                "Decoded DATA exceeds negotiated file size",
            ));
        }
        file.write_all(&buffer[..count]).map_err(|e| TrzszError {
            message: e.to_string(),
            err_type: String::new(),
            trace: false,
        })?;
        hasher.update(&buffer[..count]);
        step += count as i64;
        saved.set(step);
        if let Some(callback) = progress.as_mut() {
            callback.on_step(step);
        }
    }
    if step != size {
        return Err(crate::comm::simple_trzsz_error(
            "Decoded DATA size mismatch",
            format!("{} <> {}", step, size),
        ));
    }
    Ok(hasher.finalize().to_vec())
}

fn to_io_error(error: TrzszError) -> io::Error {
    io::Error::other(error.message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex, mpsc::SyncSender};
    use std::thread;
    use std::time::{Duration, Instant};

    use base64::Engine;

    #[derive(Clone, Default)]
    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for CaptureWriter {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(data);
            Ok(data.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct TestFileWriter;

    impl FileWriter for TestFileWriter {
        fn write_all(&mut self, _data: &[u8]) -> io::Result<()> {
            Ok(())
        }

        fn close(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn count_data_frames(output: &[u8]) -> usize {
        output
            .windows(b"#DATA:".len())
            .filter(|window| *window == b"#DATA:")
            .count()
    }

    fn wait_for_frames(output: &Arc<Mutex<Vec<u8>>>, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let count = count_data_frames(&output.lock().unwrap());
            if count >= expected {
                assert_eq!(count, expected);
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for DATA frames"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn send_ack(sender: &SyncSender<Vec<u8>>, length: usize, step: i64) {
        sender
            .send(format!("#SUCC:{}/{}\n", length, step).into_bytes())
            .unwrap();
    }

    #[test]
    fn sender_limits_in_flight_data_to_five_frames() {
        let output = CaptureWriter::default();
        let captured = output.0.clone();
        let mut transfer = TrzszTransfer::new(Box::new(output));
        transfer.transfer_config.bufsize = 1024;
        transfer.transfer_config.timeout = 2;
        let input = transfer.buffer.sender();

        let worker = thread::spawn(move || {
            let mut progress = None;
            let mut writer = FrameWriter::new(&mut transfer, false, 6 * 1024, &mut progress);
            writer.write_all(&vec![b'x'; 6 * 1024]).unwrap();
            writer.finish().unwrap();
        });

        wait_for_frames(&captured, ACK_WINDOW);
        thread::sleep(Duration::from_millis(50));
        assert_eq!(count_data_frames(&captured.lock().unwrap()), ACK_WINDOW);

        send_ack(&input, 1024, 0);
        wait_for_frames(&captured, ACK_WINDOW + 1);
        send_ack(&input, 1024, 0);
        wait_for_frames(&captured, ACK_WINDOW + 2);

        for _ in 0..4 {
            send_ack(&input, 1024, 0);
        }
        send_ack(&input, 0, 0);
        input.send(b"#SUCC:6144\n".to_vec()).unwrap();
        worker.join().unwrap();
    }

    fn receive_custom_rust_v2(wire: &[u8], size: i64) -> Result<Vec<u8>, TrzszError> {
        receive_custom_rust_v2_with_config(wire, size, false, 10 * 1024 * 1024, 2)
    }

    fn receive_custom_rust_v2_with_config(
        wire: &[u8],
        size: i64,
        binary: bool,
        bufsize: i64,
        timeout: i32,
    ) -> Result<Vec<u8>, TrzszError> {
        let mut transfer = TrzszTransfer::new(Box::new(io::sink()));
        transfer.transfer_config.protocol = 2;
        transfer.transfer_config.lang = "rust".to_string();
        transfer.transfer_config.binary = binary;
        transfer.transfer_config.bufsize = bufsize;
        transfer.transfer_config.timeout = timeout;
        transfer.peer_lang = "rust".to_string();
        transfer.add_received_data(wire, false);
        super::recv_file_data(&mut transfer, &mut TestFileWriter, size, &mut None)
    }

    #[test]
    fn malformed_base64_v2_stream_returns_error() {
        let wire = b"#COMP:true\n#DATA:%%%\n#DATA:\n";
        let error = receive_custom_rust_v2(wire, 32).unwrap_err();
        assert!(error.message.contains("V2 data stream"));
    }

    #[test]
    fn truncated_zstd_v2_stream_returns_error() {
        let full = zstd::stream::encode_all(&b"zstd payload for truncated frame"[..], 0).unwrap();
        let truncated = &full[..full.len() - 1];
        let encoded = BASE64.encode(truncated);
        let wire = format!("#COMP:true\n#DATA:{}\n#DATA:\n", encoded);
        let error = receive_custom_rust_v2(wire.as_bytes(), 32).unwrap_err();
        assert!(error.message.contains("V2 data stream"));
    }

    #[test]
    fn oversized_base64_frame_is_rejected_during_line_read() {
        let mut wire = b"#COMP:true\n#DATA:".to_vec();
        wire.extend(std::iter::repeat_n(b'A', 4096));
        let error = receive_custom_rust_v2_with_config(&wire, 32, false, 1024, 0).unwrap_err();
        assert!(error.message.contains("maximum size"));
    }

    #[test]
    fn oversized_binary_frame_is_rejected_before_allocation() {
        let wire = b"#COMP:false\n#DATA:9223372036854775807\n";
        let error = receive_custom_rust_v2_with_config(wire, 1, true, i64::MAX, 0).unwrap_err();
        assert!(
            error
                .message
                .contains("DATA frame exceeds negotiated limit")
        );
    }

    #[test]
    fn v3_auto_compression_thresholds_match_go() {
        let mut transfer = TrzszTransfer::new(Box::new(io::sink()));
        transfer.transfer_config.protocol = 3;
        assert_eq!(v3_fixed_compression(&transfer, 511), Some(false));
        assert_eq!(v3_fixed_compression(&transfer, 512), Some(true));
        assert_eq!(v3_fixed_compression(&transfer, 128 * 1024 - 1), Some(true));
        assert_eq!(v3_fixed_compression(&transfer, 128 * 1024), None);

        transfer.transfer_config.compress = CompressType::Yes as i32;
        assert_eq!(v3_fixed_compression(&transfer, 1), Some(true));
        transfer.transfer_config.compress = CompressType::No as i32;
        assert_eq!(v3_fixed_compression(&transfer, i64::MAX), Some(false));
    }

    #[test]
    fn go_v2_compressed_text_does_not_require_comp_line() {
        let data = b"Go protocol 2 uses zstd for text without COMP";
        let compressed = zstd::stream::encode_all(&data[..], 0).unwrap();
        let encoded = BASE64.encode(compressed);
        let wire = format!("#DATA:{encoded}\n#DATA:\n");
        let mut transfer = TrzszTransfer::new(Box::new(io::sink()));
        transfer.transfer_config.protocol = 2;
        transfer.transfer_config.lang = "go".to_string();
        transfer.transfer_config.timeout = 2;
        transfer.peer_lang = "go".to_string();
        transfer.add_received_data(wire.as_bytes(), false);

        let digest = recv_file_data(
            &mut transfer,
            &mut TestFileWriter,
            data.len() as i64,
            &mut None,
        )
        .unwrap();
        assert_eq!(digest, Md5::digest(data).to_vec());
    }
}
