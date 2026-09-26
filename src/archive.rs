use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::comm::{FileReader, FileWriter, SourceFile};
use crate::escape;

const MAX_ARCHIVE_HEADER_SIZE: usize = 16 * 1024 * 1024;

pub(crate) struct ArchiveFileReader {
    files: Vec<SourceFile>,
    headers: Vec<Vec<u8>>,
    path_id: i32,
    index: usize,
    started: bool,
    header_offset: usize,
    file: Option<File>,
    remaining: i64,
    size: i64,
}

impl ArchiveFileReader {
    pub(crate) fn new(files: Vec<SourceFile>, path_id: i32) -> io::Result<Self> {
        let mut headers = Vec::with_capacity(files.len());
        let mut size = 0_i64;
        for file in &files {
            if file.path_id != path_id || file.size < 0 || (file.is_dir && file.size != 0) {
                return Err(invalid_data("Invalid source file in archive"));
            }
            let serialized = file
                .marshal()
                .map_err(|error| invalid_data(format!("Marshal archive header failed: {error}")))?;
            let mut header = escape::encode_string(&serialized).into_bytes();
            header.push(b'\n');
            size = size
                .checked_add(
                    i64::try_from(header.len())
                        .map_err(|_| invalid_data("Archive size overflow"))?,
                )
                .and_then(|size| {
                    if file.is_dir {
                        Some(size)
                    } else {
                        size.checked_add(file.size)
                    }
                })
                .ok_or_else(|| invalid_data("Archive size overflow"))?;
            headers.push(header);
        }
        Ok(Self {
            files,
            headers,
            path_id,
            index: 0,
            started: false,
            header_offset: 0,
            file: None,
            remaining: 0,
            size,
        })
    }

    fn start_file(&mut self) -> io::Result<()> {
        let source = &self.files[self.index];
        if source.path_id != self.path_id || source.size < 0 || (source.is_dir && source.size != 0)
        {
            return Err(invalid_data("Invalid source file in archive"));
        }
        self.remaining = source.size;
        if source.is_dir {
            self.file = None;
        } else {
            self.file = Some(File::open(&source.abs_path)?);
        }
        self.header_offset = 0;
        self.started = true;
        Ok(())
    }
}

impl FileReader for ArchiveFileReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut written = 0;
        while written < buf.len() {
            if self.index >= self.files.len() {
                break;
            }
            if !self.started {
                self.start_file()?;
            }
            let header = &self.headers[self.index];
            if self.header_offset < header.len() {
                let count = (header.len() - self.header_offset).min(buf.len() - written);
                buf[written..written + count]
                    .copy_from_slice(&header[self.header_offset..self.header_offset + count]);
                self.header_offset += count;
                written += count;
                continue;
            }
            if self.remaining > 0 {
                let file = self
                    .file
                    .as_mut()
                    .ok_or_else(|| invalid_data("Missing archive source file"))?;
                let count = usize::try_from(self.remaining)
                    .unwrap_or(usize::MAX)
                    .min(buf.len() - written);
                let read = file.read(&mut buf[written..written + count])?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Unexpected EOF in archive source file",
                    ));
                }
                self.remaining -= read as i64;
                written += read;
                continue;
            }
            self.file = None;
            self.index += 1;
            self.started = false;
        }
        Ok(written)
    }

    fn size(&self) -> i64 {
        self.size
    }

    fn close(&mut self) -> io::Result<()> {
        self.file = None;
        Ok(())
    }
}

pub(crate) struct ArchiveFileWriter {
    root: PathBuf,
    path_id: i32,
    header: Vec<u8>,
    file: Option<File>,
    remaining: u64,
    entries: usize,
}

impl ArchiveFileWriter {
    pub(crate) fn new(root: PathBuf, path_id: i32) -> Self {
        Self {
            root,
            path_id,
            header: Vec::new(),
            file: None,
            remaining: 0,
            entries: 0,
        }
    }

    fn write_entry_header(&mut self, header: &[u8]) -> io::Result<()> {
        let encoded = std::str::from_utf8(header)
            .map_err(|error| invalid_data(format!("Invalid archive header encoding: {error}")))?;
        let decoded = escape::decode_string(encoded)
            .map_err(|error| invalid_data(format!("Decode archive header failed: {error}")))?;
        let json = std::str::from_utf8(&decoded)
            .map_err(|error| invalid_data(format!("Invalid archive JSON encoding: {error}")))?;
        let source: SourceFile = serde_json::from_str(json)
            .map_err(|error| invalid_data(format!("Invalid archive source file: {error}")))?;
        validate_archive_entry(&source, self.path_id)?;
        validate_archive_parent(&self.root, &source)?;
        let path = archive_entry_path(&self.root, &source)?;
        if source.is_dir {
            create_archive_directory(&path, source.perm.unwrap_or(0))?;
        } else {
            let parent = path
                .parent()
                .ok_or_else(|| invalid_data("Archive file has no parent directory"))?;
            if !parent.is_dir() {
                return Err(invalid_data("Archive parent directory is missing"));
            }
            let file = create_archive_file(&path, source.perm.unwrap_or(0))?;
            self.remaining = source.size as u64;
            self.file = Some(file);
            if self.remaining == 0 {
                self.file = None;
            }
        }
        self.entries += 1;
        Ok(())
    }
}

impl FileWriter for ArchiveFileWriter {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        let mut offset = 0;
        while offset < buf.len() {
            if self.remaining > 0 {
                let count = usize::try_from(self.remaining)
                    .unwrap_or(usize::MAX)
                    .min(buf.len() - offset);
                let file = self
                    .file
                    .as_mut()
                    .ok_or_else(|| invalid_data("Missing archive destination file"))?;
                file.write_all(&buf[offset..offset + count])?;
                self.remaining -= count as u64;
                offset += count;
                if self.remaining == 0 {
                    self.file = None;
                }
                continue;
            }
            if let Some(index) = buf[offset..].iter().position(|byte| *byte == b'\n') {
                if self.header.len() + index > MAX_ARCHIVE_HEADER_SIZE {
                    return Err(invalid_data("Archive header is too large"));
                }
                self.header.extend_from_slice(&buf[offset..offset + index]);
                let header = std::mem::take(&mut self.header);
                self.write_entry_header(&header)?;
                offset += index + 1;
            } else {
                if self.header.len() + buf.len() - offset > MAX_ARCHIVE_HEADER_SIZE {
                    return Err(invalid_data("Archive header is too large"));
                }
                self.header.extend_from_slice(&buf[offset..]);
                break;
            }
        }
        Ok(())
    }

    fn close(&mut self) -> io::Result<()> {
        if self.remaining != 0 || !self.header.is_empty() || self.entries == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Truncated archive data",
            ));
        }
        if let Some(file) = self.file.as_mut() {
            file.flush()?;
        }
        self.file = None;
        Ok(())
    }

    fn size(&self) -> io::Result<u64> {
        Ok(0)
    }
}

fn validate_archive_entry(source: &SourceFile, path_id: i32) -> io::Result<()> {
    if source.path_id != path_id
        || source.rel_path.len() < 2
        || source.size < 0
        || source.archive
        || (source.is_dir && source.size != 0)
    {
        return Err(invalid_data("Invalid source file in archive"));
    }
    for part in &source.rel_path {
        validate_archive_path_part(part)?;
    }
    Ok(())
}
pub(crate) fn validate_archive_root(name: &str) -> io::Result<()> {
    validate_archive_path_part(name)
}

fn validate_archive_path_part(part: &str) -> io::Result<()> {
    let mut components = Path::new(part).components();
    if part.is_empty()
        || part == "."
        || part == ".."
        || part.contains(['/', '\\'])
        || !matches!(components.next(), Some(std::path::Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(invalid_data("Invalid path in archive"));
    }
    Ok(())
}

fn validate_archive_parent(root: &Path, source: &SourceFile) -> io::Result<()> {
    let mut parent = root.to_path_buf();
    for part in &source.rel_path[1..source.rel_path.len() - 1] {
        parent.push(part);
        let metadata = fs::symlink_metadata(&parent)
            .map_err(|_| invalid_data("Archive parent directory is missing"))?;
        if !metadata.file_type().is_dir() {
            return Err(invalid_data("Archive parent is not a directory"));
        }
    }
    Ok(())
}
fn create_archive_file(path: &Path, perm: u32) -> io::Result<File> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => return Err(invalid_data("Archive file conflicts with existing path")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode((perm | 0o600) & 0o777);
    }
    options.open(path)
}

fn archive_entry_path(root: &Path, source: &SourceFile) -> io::Result<PathBuf> {
    let mut path = root.to_path_buf();
    for part in &source.rel_path[1..] {
        path.push(part);
    }
    Ok(path)
}

fn create_archive_directory(path: &Path, perm: u32) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => return Ok(()),
        Ok(_) => {
            return Err(invalid_data(
                "Archive directory conflicts with existing file",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let parent = path
        .parent()
        .ok_or_else(|| invalid_data("Archive directory has no parent"))?;
    if !parent.is_dir() {
        return Err(invalid_data("Archive parent directory is missing"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = fs::DirBuilder::new();
        builder.mode((perm | 0o700) & 0o777).create(path)?;
    }
    #[cfg(not(unix))]
    fs::create_dir(path)?;
    Ok(())
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn snapshot(root: &Path) -> BTreeMap<String, Option<Vec<u8>>> {
        fn visit(root: &Path, current: &Path, files: &mut BTreeMap<String, Option<Vec<u8>>>) {
            for entry in fs::read_dir(current).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                let name = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                if path.is_dir() {
                    files.insert(name, None);
                    visit(root, &path, files);
                } else {
                    files.insert(name, Some(fs::read(path).unwrap()));
                }
            }
        }
        let mut files = BTreeMap::new();
        visit(root, root, &mut files);
        files
    }

    fn sample_directory(root: &Path) -> Vec<SourceFile> {
        fs::create_dir_all(root.join("nested/空")).unwrap();
        fs::create_dir(root.join("empty-dir")).unwrap();
        fs::write(root.join("empty-file"), []).unwrap();
        fs::write(root.join("nested/空/文件.txt"), "directory archive 😀\n").unwrap();
        crate::comm::check_paths_readable(&[root.to_path_buf()], true).unwrap()
    }

    fn copy_archive(reader: &mut ArchiveFileReader, writer: &mut ArchiveFileWriter, chunk: usize) {
        let mut data = vec![0; chunk];
        loop {
            let count = reader.read(&mut data).unwrap();
            if count == 0 {
                break;
            }
            writer.write_all(&data[..count]).unwrap();
        }
        writer.close().unwrap();
    }

    #[test]
    fn archive_round_trip_across_data_chunk_boundaries() {
        let source = tempfile::tempdir().unwrap();
        let source_root = source.path().join("bundle");
        let sources = sample_directory(&source_root);
        let children = sources[1..].to_vec();
        let expected = snapshot(&source_root);

        for chunk in [1, 7, 31, 4096] {
            let destination = tempfile::tempdir().unwrap();
            let destination_root = destination.path().join("bundle");
            fs::create_dir(&destination_root).unwrap();
            let mut reader = ArchiveFileReader::new(children.clone(), 0).unwrap();
            let mut writer = ArchiveFileWriter::new(destination_root.clone(), 0);
            copy_archive(&mut reader, &mut writer, chunk);
            assert_eq!(snapshot(&destination_root), expected, "chunk size {chunk}");
        }
    }

    #[test]
    fn archive_writer_rejects_invalid_truncated_and_escaping_headers() {
        let destination = tempfile::tempdir().unwrap();
        let root = destination.path().join("bundle");
        fs::create_dir(&root).unwrap();

        let mut invalid = ArchiveFileWriter::new(root.clone(), 0);
        assert!(invalid.write_all(b"not-an-archive-header\n").is_err());

        let mut truncated = ArchiveFileWriter::new(root.clone(), 0);
        truncated.write_all(b"partial-header").unwrap();
        assert_eq!(
            truncated.close().unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );

        let escaping = SourceFile {
            path_id: 0,
            abs_path: PathBuf::new(),
            rel_path: vec!["bundle".into(), "..".into(), "escaped.txt".into()],
            is_dir: false,
            archive: false,
            sub_files: Vec::new(),
            size: 0,
            perm: None,
        };
        let header = format!("{}\n", escape::encode_string(&escaping.marshal().unwrap()));
        let mut traversal = ArchiveFileWriter::new(root, 0);
        assert!(traversal.write_all(header.as_bytes()).is_err());
    }
}
