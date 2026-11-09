use std::{
    collections::HashSet,
    ffi::OsStr,
    path::{Path, PathBuf},
    sync::Arc,
};

#[cfg(feature = "tracing")]
use tracing::{debug, warn};

use crate::{
    config::JournalConfig,
    cursor::Cursor,
    error::{LimitKind, Result, SdJournalError},
    file::{EntryRange, JournalFile},
    live::LiveJournal,
    query::JournalQuery,
};

/// An opened set of journal files.
///
/// Cloning a `Journal` is cheap; the underlying journal-file set is reference counted.
#[derive(Clone)]
pub struct Journal {
    pub(crate) inner: Arc<JournalInner>,
}

pub(crate) struct JournalInner {
    #[cfg(unix)]
    pub(crate) reader_cache: Arc<crate::file::ReaderCache>,
    pub(crate) config: JournalConfig,
    pub(crate) roots: Vec<PathBuf>,
    pub(crate) file_infos: Vec<JournalFileInfo>,
    pub(crate) open_errors: Vec<(PathBuf, SdJournalError)>,
}

#[derive(Clone)]
pub(crate) struct JournalFileInfo {
    pub(crate) path: PathBuf,
    pub(crate) file_id: [u8; 16],
    pub(crate) seqnum_id: [u8; 16],
    pub(crate) entry_range: Option<EntryRange>,
    pub(crate) entry_range_known: bool,
    file: JournalFile,
    keep_open: bool,
    used_size: u64,
    #[cfg(unix)]
    file_identity: (u64, u64),
}

pub(crate) struct JournalDiscovery {
    pub(crate) roots: Vec<PathBuf>,
    pub(crate) candidates: Vec<PathBuf>,
}

impl Journal {
    /// Open the default system journal roots on Linux.
    ///
    /// This scans `/run/log/journal` and `/var/log/journal`, opens any discovered journal files,
    /// and deduplicates them by journal file ID.
    ///
    /// On non-Linux targets this returns [`SdJournalError::Unsupported`].
    pub fn open_default() -> Result<Self> {
        Self::open_default_with_config(JournalConfig::default())
    }

    /// Open the default system journal roots with a custom [`JournalConfig`].
    ///
    /// See [`Journal::open_default`] for the scanned locations and platform behavior.
    pub fn open_default_with_config(config: JournalConfig) -> Result<Self> {
        core::cfg_select! {
            target_os = "linux" => {
                let paths = vec![
                    PathBuf::from("/run/log/journal"),
                    PathBuf::from("/var/log/journal"),
                ];
                Self::open_dirs_with_config(&paths, config)
            }
            _ => {
                let _ = config;
                Err(SdJournalError::Unsupported {
                    reason: "Journal::open_default is only supported on Linux".to_string(),
                })
            }
        }
    }

    /// Open journal files discovered under a single root directory.
    ///
    /// Discovery includes `*.journal` files in the directory itself and in immediate machine-ID
    /// subdirectories such as `/var/log/journal/<machine-id>/`.
    pub fn open_dir(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_dir_with_config(path, JournalConfig::default())
    }

    /// Open journal files discovered under a single root directory with a custom configuration.
    pub fn open_dir_with_config(path: impl AsRef<Path>, config: JournalConfig) -> Result<Self> {
        let paths = vec![path.as_ref().to_path_buf()];
        Self::open_dirs_with_config(&paths, config)
    }

    /// Open journal files discovered under multiple root directories.
    ///
    /// Roots are sorted and deduplicated before discovery.
    pub fn open_dirs(paths: &[PathBuf]) -> Result<Self> {
        Self::open_dirs_with_config(paths, JournalConfig::default())
    }

    /// Open journal files discovered under multiple root directories using a custom configuration.
    ///
    /// Discovery skips symlinks, only keeps files with supported journal extensions, and
    /// deduplicates opened files by journal file ID.
    ///
    /// When discovery finds more files than [`JournalConfig::max_open_files`], the returned
    /// [`Journal`] stores file paths and opens files on demand while queries perform a streaming
    /// merge. Combine this with [`crate::MmapPolicy::Never`] for strict virtual-memory limits.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`SdJournalError::InvalidQuery`] when `paths` is empty.
    /// - [`SdJournalError::NotFound`] when no usable journal files are found.
    /// - [`SdJournalError::LimitExceeded`] when discovery exceeds configured file limits.
    /// - An underlying file or parse error when all candidates fail to open.
    pub fn open_dirs_with_config(paths: &[PathBuf], config: JournalConfig) -> Result<Self> {
        validate_journal_config(&config)?;
        let discovery = discover_journal_candidates(paths, &config)?;
        let file_set = open_journal_file_set(discovery.candidates, &config)?;

        #[cfg(feature = "tracing")]
        debug!(
            n_files = file_set.file_infos.len(),
            lazy = file_set.file_infos.iter().any(|info| !info.keep_open),
            "journal opened"
        );

        Ok(Self {
            inner: Arc::new(JournalInner {
                #[cfg(unix)]
                reader_cache: crate::file::ReaderCache::new(&config),
                config,
                roots: discovery.roots,
                file_infos: file_set.file_infos,
                open_errors: file_set.errors,
            }),
        })
    }

    /// Start building a query over the currently opened journal files.
    pub fn query(&self) -> JournalQuery {
        JournalQuery::new(self.clone())
    }

    /// Individual files excluded because opening them failed.
    ///
    /// Opening remains best-effort when other files are usable. This borrowed
    /// list retains their actual discovery paths and typed errors, independent
    /// of the optional tracing feature. Its length is bounded by
    /// `JournalConfig::max_journal_files`. Retirement is returned directly for
    /// rediscovery; corruption while iterating included data is not skipped.
    pub fn open_errors(&self) -> &[(PathBuf, SdJournalError)] {
        &self.inner.open_errors
    }

    /// Create a shared live engine from this journal set.
    ///
    /// The returned [`LiveJournal`] shares one watcher and one live cursor across all registered
    /// subscriptions.
    pub fn live(&self) -> Result<LiveJournal> {
        LiveJournal::from_journal(self.clone())
    }

    /// Create a query positioned at `cursor`, inclusive.
    ///
    /// This is a convenience wrapper around [`Journal::query`] plus cursor positioning.
    pub fn seek_cursor(&self, cursor: &Cursor) -> Result<JournalQuery> {
        let mut q = self.query();
        q.set_cursor_start(cursor.clone(), true)?;
        Ok(q)
    }

    /// Verify Forward Secure Sealing (TAG objects) for all opened journal files.
    ///
    /// This validates TAG objects against a systemd verification key and returns an error if any
    /// opened file is sealed but fails verification.
    ///
    /// This is available when the `verify-seal` feature is enabled.
    #[cfg(feature = "verify-seal")]
    pub fn verify_seal(&self, verification_key: &str) -> Result<()> {
        let key = crate::seal::parse_verification_key(verification_key)?;
        let params = crate::seal::FsprgParams::new(key.seed())?;

        for idx in 0..self.inner.file_count() {
            let f = self.inner.open_file_by_index(idx)?;
            crate::seal::verify_file_seal(&f, &params, &key)?;
        }
        Ok(())
    }
}

pub(crate) struct JournalFileSet {
    pub(crate) file_infos: Vec<JournalFileInfo>,
    errors: Vec<(PathBuf, SdJournalError)>,
}

impl JournalInner {
    pub(crate) fn validate_snapshot_owners(&self) -> Result<()> {
        for info in &self.file_infos {
            if info.keep_open {
                info.file.validate_snapshot()?;
            } else {
                let metadata = std::fs::metadata(&info.path)
                    .map_err(|error| crate::file::snapshot_open_error(&info.path, error))?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt as _;
                    if (metadata.dev(), metadata.ino()) != info.file_identity {
                        return Err(SdJournalError::Retired {
                            path: info.path.clone(),
                            reason: crate::RetirementReason::Replaced,
                        });
                    }
                }
                #[cfg(not(unix))]
                let _ = &metadata;
                if metadata.len() < info.used_size {
                    return Err(SdJournalError::Transient {
                        path: Some(info.path.clone()),
                        reason: "file was truncated while reading".to_owned(),
                    });
                }
            }
        }
        Ok(())
    }
    pub(crate) fn is_lazy(&self) -> bool {
        self.file_infos.iter().any(|info| !info.keep_open)
    }

    pub(crate) fn file_count(&self) -> usize {
        self.file_infos.len()
    }

    pub(crate) fn file_info(&self, idx: usize) -> Option<&JournalFileInfo> {
        self.file_infos.get(idx)
    }

    pub(crate) fn file_paths(&self) -> Vec<PathBuf> {
        self.file_infos
            .iter()
            .map(|info| info.path.clone())
            .collect()
    }

    pub(crate) fn opened_files(&self) -> Option<Vec<JournalFile>> {
        if self.is_lazy() {
            return None;
        }
        Some(
            self.file_infos
                .iter()
                .map(|info| info.file.clone())
                .collect(),
        )
    }

    pub(crate) fn open_file_by_index(&self, idx: usize) -> Result<JournalFile> {
        let info = self.file_infos.get(idx).ok_or(SdJournalError::NotFound)?;
        if info.keep_open {
            return Ok(info.file.clone());
        }
        #[cfg(unix)]
        let file = self.reader_cache.open_file(&info.file)?;
        #[cfg(not(unix))]
        let file = {
            info.file.validate_snapshot()?;
            info.file.clone()
        };
        Ok(file)
    }
}

impl JournalFileInfo {
    fn from_open_file(file: JournalFile, keep_open: bool) -> Self {
        let used_size = file.live_state().used_size;
        #[cfg(unix)]
        let file_identity = file.file_identity();
        let path = file.path().to_path_buf();
        let file_id = file.file_id();
        let seqnum_id = file.seqnum_id();
        let entry_range_result = file.entry_range();
        let entry_range_known = entry_range_result.is_ok();
        let entry_range = entry_range_result.ok().flatten();

        Self {
            path,
            file_id,
            seqnum_id,
            entry_range,
            entry_range_known,
            file: if keep_open {
                file
            } else {
                file.reopen_snapshot()
            },
            keep_open,
            used_size,
            #[cfg(unix)]
            file_identity,
        }
    }
}

pub(crate) fn journal_from_snapshot_files(
    roots: Vec<PathBuf>,
    files: &[JournalFile],
    config: JournalConfig,
) -> Result<Journal> {
    validate_journal_config(&config)?;
    let mut seen = HashSet::new();
    let file_infos: Vec<JournalFileInfo> = files
        .iter()
        .filter(|file| seen.insert(file.file_id()))
        .map(|file| JournalFileInfo::from_open_file(file.reopen_snapshot(), true))
        .collect();
    if file_infos.is_empty() {
        return Err(SdJournalError::NotFound);
    }
    Ok(Journal {
        inner: Arc::new(JournalInner {
            #[cfg(unix)]
            reader_cache: crate::file::ReaderCache::new(&config),
            config,
            roots,
            file_infos,
            open_errors: Vec::new(),
        }),
    })
}

fn validate_journal_config(config: &JournalConfig) -> Result<()> {
    if config.max_open_files == 0 {
        return Err(SdJournalError::InvalidQuery {
            reason: "max_open_files must be greater than zero".to_string(),
        });
    }
    Ok(())
}

fn open_journal_file_set(paths: Vec<PathBuf>, config: &JournalConfig) -> Result<JournalFileSet> {
    if paths.len() <= config.max_open_files {
        open_eager_file_set(paths, config)
    } else {
        open_lazy_file_set(paths, config)
    }
}

fn open_eager_file_set(paths: Vec<PathBuf>, config: &JournalConfig) -> Result<JournalFileSet> {
    let mut file_infos = Vec::new();
    let mut errors = Vec::new();
    let mut seen: HashSet<[u8; 16]> = HashSet::new();
    for path in paths {
        match JournalFile::open(path.clone(), config) {
            Ok(f) => {
                #[cfg(feature = "tracing")]
                debug!(path = %f.path().display(), "opened journal file");
                let info = JournalFileInfo::from_open_file(f, true);
                if seen.insert(info.file_id) {
                    file_infos.push(info);
                }
            }
            Err(e @ SdJournalError::Retired { .. }) => return Err(e),
            Err(e) => {
                #[cfg(feature = "tracing")]
                warn!(error = %e, "failed to open journal file");
                errors.push((path, e))
            }
        }
    }

    if file_infos.is_empty() {
        return Err(errors
            .into_iter()
            .next()
            .map(|(_, error)| error)
            .unwrap_or(SdJournalError::NotFound));
    }

    Ok(JournalFileSet { file_infos, errors })
}

fn open_lazy_file_set(paths: Vec<PathBuf>, config: &JournalConfig) -> Result<JournalFileSet> {
    let mut file_infos = Vec::new();
    let mut errors = Vec::new();
    let mut seen: HashSet<[u8; 16]> = HashSet::new();

    for path in paths {
        match JournalFile::open(path.clone(), config) {
            Ok(file) => {
                let info = JournalFileInfo::from_open_file(file, false);
                if seen.insert(info.file_id) {
                    file_infos.push(info);
                }
            }
            Err(e @ SdJournalError::Retired { .. }) => return Err(e),
            Err(e) => {
                #[cfg(feature = "tracing")]
                warn!(error = %e, "failed to validate journal file for lazy open");
                errors.push((path, e));
            }
        }
    }

    if file_infos.is_empty() {
        return Err(errors
            .into_iter()
            .next()
            .map(|(_, error)| error)
            .unwrap_or(SdJournalError::NotFound));
    }

    Ok(JournalFileSet { file_infos, errors })
}

pub(crate) fn discover_journal_candidates(
    paths: &[PathBuf],
    config: &JournalConfig,
) -> Result<JournalDiscovery> {
    if paths.is_empty() {
        return Err(SdJournalError::InvalidQuery {
            reason: "open_dirs requires at least one path".to_string(),
        });
    }

    let mut roots = paths.to_vec();
    roots.sort_by(|a, b| a.to_string_lossy().cmp(&b.to_string_lossy()));
    roots.dedup();

    let mut candidates: Vec<PathBuf> = Vec::new();
    let mut first_discover_error: Option<SdJournalError> = None;
    for p in paths {
        if let Err(e) = discover_journal_paths(p, config, &mut candidates)
            && first_discover_error.is_none()
        {
            #[cfg(feature = "tracing")]
            warn!(path = %p.display(), error = %e, "journal discovery failed");
            first_discover_error = Some(e);
        }
    }

    if candidates.is_empty() {
        return Err(first_discover_error.unwrap_or(SdJournalError::NotFound));
    }

    candidates.sort_by(|a, b| a.to_string_lossy().cmp(&b.to_string_lossy()));
    candidates.dedup();

    if candidates.len() > config.max_journal_files {
        return Err(SdJournalError::LimitExceeded {
            kind: LimitKind::JournalFiles,
            limit: u64::try_from(config.max_journal_files).unwrap_or(u64::MAX),
        });
    }

    Ok(JournalDiscovery { roots, candidates })
}

fn discover_journal_paths(
    dir: &Path,
    config: &JournalConfig,
    out: &mut Vec<PathBuf>,
) -> Result<()> {
    let rd = std::fs::read_dir(dir)
        .map_err(|e| SdJournalError::io("read_dir", Some(dir.to_path_buf()), e))?;

    let mut machine_id_dirs: Vec<PathBuf> = Vec::new();

    for entry in rd {
        let entry =
            entry.map_err(|e| SdJournalError::io("read_dir_entry", Some(dir.to_path_buf()), e))?;
        let ft = entry
            .file_type()
            .map_err(|e| SdJournalError::io("file_type", Some(entry.path()), e))?;

        if ft.is_symlink() {
            continue;
        }

        if ft.is_dir() {
            let name = entry.file_name();
            if is_machine_id_dir(&name) {
                machine_id_dirs.push(entry.path());
            }
            continue;
        }

        if !ft.is_file() {
            continue;
        }

        if is_journal_file(&entry.path(), config) {
            out.push(entry.path());
        }
    }

    for mid in machine_id_dirs {
        let rd = match std::fs::read_dir(&mid) {
            Ok(rd) => rd,
            Err(e) => return Err(SdJournalError::io("read_dir", Some(mid), e)),
        };
        for entry in rd {
            let entry = entry
                .map_err(|e| SdJournalError::io("read_dir_entry", Some(dir.to_path_buf()), e))?;
            let ft = entry
                .file_type()
                .map_err(|e| SdJournalError::io("file_type", Some(entry.path()), e))?;
            if ft.is_symlink() || !ft.is_file() {
                continue;
            }
            if is_journal_file(&entry.path(), config) {
                out.push(entry.path());
            }
        }
    }

    Ok(())
}

fn is_machine_id_dir(name: &OsStr) -> bool {
    let s = match name.to_str() {
        Some(s) => s,
        None => return false,
    };
    if s.len() != 32 {
        return false;
    }
    s.bytes().all(|b| b.is_ascii_hexdigit())
}

fn is_journal_file(path: &Path, config: &JournalConfig) -> bool {
    match path.extension().and_then(|e| e.to_str()) {
        Some("journal") => true,
        Some("journal~") => config.include_journal_tilde,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};

    use tempfile::tempdir;

    use super::*;

    #[allow(dead_code)]
    mod fixture {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/synthetic_journal.rs"
        ));
    }

    #[cfg(unix)]
    #[test]
    fn lazy_hot_reads_do_not_reopen_for_each_field_above_default_budget() {
        let root = tempdir().unwrap();
        let units = ["hot.service"; 8];
        for seed in 1u8..=65 {
            fixture::write_synthetic_journal_file(
                &root.path().join(format!("{seed:03}.journal")),
                &units,
                seed,
            );
        }
        let eager = Journal::open_dir_with_config(
            root.path(),
            JournalConfig {
                max_open_files: 65,
                ..JournalConfig::default()
            },
        )
        .unwrap();
        let snapshot = |entry: crate::entry::EntryRef| {
            (
                entry.cursor().unwrap().to_string(),
                entry
                    .iter_fields()
                    .map(|(k, v)| (k.to_owned(), v.to_vec()))
                    .collect::<Vec<_>>(),
            )
        };
        let expected: Vec<_> = eager
            .query()
            .iter()
            .unwrap()
            .map(|v| snapshot(v.unwrap()))
            .collect();
        assert_eq!(expected.len(), 65 * units.len());
        drop(eager);
        let lazy = Journal::open_dir(root.path()).unwrap();
        assert!(lazy.inner.is_lazy());
        let mut iter = lazy.query().iter().unwrap();
        crate::reader::test_io::reset();
        let hot: Vec<_> = iter
            .by_ref()
            .take(4)
            .map(|v| snapshot(v.unwrap()))
            .collect();
        assert_eq!(hot, expected[..4]);
        let opens = crate::reader::test_io::opens(&root.path().join("001.journal"));
        assert!(
            opens <= 1,
            "four hot entries reopened their file {opens} times"
        );
        let rest: Vec<_> = iter.map(|v| snapshot(v.unwrap())).collect();
        assert_eq!(rest, expected[4..]);
    }

    #[test]
    fn open_dirs_rejects_empty_path_list() {
        match Journal::open_dirs_with_config(&[], JournalConfig::default()) {
            Err(SdJournalError::InvalidQuery { reason }) => {
                assert_eq!(reason, "open_dirs requires at least one path");
            }
            Ok(_) => panic!("expected InvalidQuery"),
            Err(other) => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn retirement_between_discovery_and_open_is_not_silently_excluded() {
        for max_open_files in [1, 64] {
            let root = tempdir().unwrap();
            let healthy = root.path().join("healthy.journal");
            let retired = root.path().join("retired.journal");
            fixture::write_synthetic_journal_file(&healthy, &["healthy.service"], 1);
            fixture::write_synthetic_journal_file(&retired, &["retired.service"], 2);
            let mut candidates = Vec::new();
            let config = JournalConfig {
                max_open_files,
                ..JournalConfig::default()
            };
            discover_journal_paths(root.path(), &config, &mut candidates).unwrap();
            fs::remove_file(&retired).unwrap();
            assert!(matches!(
                open_journal_file_set(candidates, &config),
                Err(SdJournalError::Retired {
                    path,
                    reason: crate::RetirementReason::Disappeared,
                }) if path == retired
            ));
        }
    }

    #[test]
    fn machine_id_dir_requires_exactly_32_hex_chars() {
        assert!(is_machine_id_dir(OsStr::new(
            "0123456789abcdef0123456789abcdef"
        )));
        assert!(!is_machine_id_dir(OsStr::new(
            "0123456789abcdef0123456789abcde"
        )));
        assert!(!is_machine_id_dir(OsStr::new(
            "0123456789abcdef0123456789abcdeg"
        )));
        assert!(!is_machine_id_dir(OsStr::new("not-a-machine-id")));
    }

    #[test]
    fn discover_journal_paths_finds_root_and_machine_id_files() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        let machine_id = root.join("0123456789abcdef0123456789abcdef");
        let ignored_dir = root.join("not-machine-id");

        fs::create_dir(&machine_id).unwrap();
        fs::create_dir(&ignored_dir).unwrap();

        let root_journal = root.join("root.journal");
        let nested_journal = machine_id.join("nested.journal");
        let tilde_journal = root.join("temp.journal~");
        let ignored_journal = ignored_dir.join("ignored.journal");
        let not_journal = root.join("notes.log");

        File::create(&root_journal).unwrap();
        File::create(&nested_journal).unwrap();
        File::create(&tilde_journal).unwrap();
        File::create(&ignored_journal).unwrap();
        File::create(&not_journal).unwrap();

        #[cfg(unix)]
        std::os::unix::fs::symlink(&root_journal, root.join("link.journal")).unwrap();

        let mut out = Vec::new();
        discover_journal_paths(root, &JournalConfig::default(), &mut out).unwrap();
        out.sort();

        assert_eq!(out, vec![nested_journal.clone(), root_journal.clone()]);

        out.clear();
        discover_journal_paths(
            root,
            &JournalConfig {
                include_journal_tilde: true,
                ..Default::default()
            },
            &mut out,
        )
        .unwrap();
        out.sort();

        assert_eq!(out, vec![nested_journal, root_journal, tilde_journal]);
    }
}
