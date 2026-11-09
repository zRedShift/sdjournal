mod filter;
mod replay;
mod tail;
#[cfg(feature = "tokio")]
mod tokio;
#[cfg(target_os = "linux")]
mod watcher;

use crate::config::{JournalConfig, LiveQueueFullPolicy};
use crate::cursor::{Cursor, SdJournalEntryKey, compare_entry_keys};
use crate::entry::{EntryRef, LiveEntry};
use crate::error::{LimitKind, Result, SdJournalError};
use crate::file::JournalFile;
use crate::journal::{Journal, discover_journal_candidates};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering as AtomicOrdering},
};
use std::thread;
use std::time::Duration;

use self::filter::CompiledFilter;
pub use self::filter::{LiveFilter, LiveOrGroupBuilder};
use self::replay::{JournalSnapshot, ReplayState, collect_replay_batch};
#[cfg(target_os = "linux")]
use self::tail::collect_watch_paths;
use self::tail::{
    FallbackDirState, TrackedFile, TrackedFiles, build_live_snapshot,
    build_tracked_files_from_open_files, build_tracked_files_from_paths, collect_fallback_dirs,
    file_metadata_changed,
};
#[cfg(feature = "tokio")]
pub use self::tokio::TokioSubscription;
#[cfg(target_os = "linux")]
use self::watcher::InotifyWatcher;

#[cfg(all(feature = "tracing", target_os = "linux"))]
use tracing::debug;
#[cfg(feature = "tracing")]
use tracing::warn;

struct SubscriptionState {
    filter: CompiledFilter,
    tx: SyncSender<Result<LiveEntry>>,
    start_after: Option<SdJournalEntryKey>,
    alive: Arc<AtomicBool>,
    replay: Option<ReplayState>,
    replay_file_tails: HashMap<[u8; 16], u64>,
    deferred_live: VecDeque<LiveEntry>,
    terminal_error: Option<SdJournalError>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct LogicalEntryId {
    seqnum_id: [u8; 16],
    seqnum: u64,
    boot_id: [u8; 16],
    monotonic_usec: u64,
    realtime_usec: u64,
    xor_hash: u64,
}

pub(super) struct WatchChange {
    pub(super) topology_changed: bool,
    pub(super) modified_paths: Vec<PathBuf>,
}

impl WatchChange {
    fn is_empty(&self) -> bool {
        !self.topology_changed && self.modified_paths.is_empty()
    }

    fn merge(&mut self, other: Self) {
        self.topology_changed |= other.topology_changed;
        self.modified_paths.extend(other.modified_paths);
        self.modified_paths.sort();
        self.modified_paths.dedup();
    }
}

enum SendOutcome {
    Delivered,
    Dropped,
    Closed,
}

struct RefreshedFile {
    tracked_idx: usize,
    next_tail: self::tail::FileTailCursor,
    next_live_state: crate::file::LiveFileState,
    next_last_key: Option<SdJournalEntryKey>,
    modified: Option<std::time::SystemTime>,
    entries: Vec<EntryRef>,
    exhausted: bool,
}

/// Shared live journal engine for multi-subscription tailing.
///
/// `LiveJournal` keeps one watcher plus lightweight per-file tail checkpoints. Ordinary appends
/// open only the modified journal file, read newly appended entries, update the checkpoint, and
/// dispatch each entry once to all matching subscriptions. Full directory rescans are reserved for
/// topology changes such as file creation, removal, or rotation.
///
/// Live delivery is bounded per cycle by [`JournalConfig::live_channel_capacity`] and
/// [`JournalConfig::max_live_batch_entries`]. Historical replay can also have a configured total
/// cap through [`JournalConfig::max_live_replay_entries`].
/// The default queue-full behavior is [`LiveQueueFullPolicy::Block`], which applies backpressure
/// instead of silently dropping entries.
///
/// Corrupt or transiently unreadable files are skipped when at least one healthy file remains;
/// enable the `tracing` feature to observe skipped-file diagnostics.
///
/// Create it through [`Journal::live`](crate::Journal::live), register one or more
/// [`LiveSubscription`]s, then drive the engine with [`LiveJournal::poll_once`] or
/// [`LiveJournal::run`].
///
/// # Model
///
/// Subscriptions are passive receivers. The engine only observes new journal data while
/// [`LiveJournal::poll_once`] or [`LiveJournal::run`] is being called. A subscription created with
/// [`LiveJournal::subscribe`] is live-only: it starts after the engine's current tail checkpoint
/// and does not replay existing entries. Use [`SubscriptionOptions`] when a replay window is
/// needed.
///
/// # Example
///
/// ```no_run
/// use sdjournal::LiveJournal;
/// use std::thread;
///
/// let mut live = LiveJournal::open_default()?;
///
/// let mut sshd_filter = live.filter();
/// sshd_filter.match_unit("sshd.service");
/// let sshd = live.subscribe(sshd_filter)?;
///
/// let mut systemd_filter = live.filter();
/// systemd_filter.match_unit("systemd.service");
/// let systemd = live.subscribe(systemd_filter)?;
///
/// let engine = thread::spawn(move || live.run());
///
/// let _entry = sshd.recv().expect("engine stopped")?;
/// let _entry = systemd.recv().expect("engine stopped")?;
///
/// drop(sshd);
/// drop(systemd);
/// engine.join().expect("live engine thread panicked")?;
/// # Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
/// ```
pub struct LiveJournal {
    roots: Vec<PathBuf>,
    config: JournalConfig,
    tracked_files: Vec<TrackedFile>,
    path_index: HashMap<PathBuf, usize>,
    fallback_dirs: Vec<FallbackDirState>,
    subscriptions: Vec<SubscriptionState>,
    last_seen: Option<SdJournalEntryKey>,
    recent_live_entries: HashSet<LogicalEntryId>,
    recent_live_order: VecDeque<LogicalEntryId>,
    live_dedup_capacity: usize,
    pending_modified_paths: Vec<PathBuf>,
    topology_refresh_requested: bool,
    next_replay_index: usize,
    prefer_replay_work: bool,
    #[cfg(target_os = "linux")]
    inotify: Option<InotifyWatcher>,
    #[cfg(target_os = "linux")]
    watch_paths: Vec<PathBuf>,
}

impl LiveJournal {
    pub(crate) fn from_journal(journal: Journal) -> Result<Self> {
        let roots = journal.inner.roots.clone();
        let config = journal.inner.config.clone();
        validate_live_config(&config)?;
        let tracked = match journal.inner.opened_files() {
            Some(files) => build_tracked_files_from_open_files(&files)?,
            None => {
                let paths = journal.inner.file_paths();
                build_tracked_files_from_paths(&paths, &config)?
            }
        };
        let live_dedup_capacity = live_dedup_capacity(&config);

        let mut out = Self {
            roots,
            config,
            tracked_files: tracked.files,
            path_index: tracked.path_index,
            fallback_dirs: Vec::new(),
            subscriptions: Vec::new(),
            last_seen: tracked.last_seen,
            recent_live_entries: HashSet::new(),
            recent_live_order: VecDeque::new(),
            live_dedup_capacity,
            pending_modified_paths: Vec::new(),
            topology_refresh_requested: false,
            next_replay_index: 0,
            prefer_replay_work: false,
            #[cfg(target_os = "linux")]
            inotify: None,
            #[cfg(target_os = "linux")]
            watch_paths: Vec::new(),
        };
        out.refresh_fallback_dirs();
        out.refresh_watchers(false);
        Ok(out)
    }

    /// Create a live engine from the default system journal roots on Linux.
    ///
    /// This avoids opening a historical [`Journal`] first. Prefer this constructor when the
    /// application only needs live tailing.
    ///
    /// On non-Linux targets this returns [`SdJournalError::Unsupported`].
    pub fn open_default() -> Result<Self> {
        Self::open_default_with_config(JournalConfig::default())
    }

    /// Create a live engine from the default system journal roots with a custom configuration.
    ///
    /// See [`LiveJournal::open_default`] for platform behavior.
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
                    reason: "LiveJournal::open_default is only supported on Linux".to_string(),
                })
            }
        }
    }

    /// Create a live engine from journal files discovered under one root directory.
    ///
    /// This is the live-tail counterpart to [`Journal::open_dir`].
    pub fn open_dir(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_dir_with_config(path, JournalConfig::default())
    }

    /// Create a live engine from one root directory with a custom configuration.
    pub fn open_dir_with_config(path: impl AsRef<Path>, config: JournalConfig) -> Result<Self> {
        let paths = vec![path.as_ref().to_path_buf()];
        Self::open_dirs_with_config(&paths, config)
    }

    /// Create a live engine from journal files discovered under multiple root directories.
    ///
    /// Unlike [`Journal::open_dirs`], this opens files one at a time during initialization and
    /// stores only lightweight tail checkpoints afterward. Journal files are opened briefly when
    /// refreshing topology or reading appended entries.
    pub fn open_dirs(paths: &[PathBuf]) -> Result<Self> {
        Self::open_dirs_with_config(paths, JournalConfig::default())
    }

    /// Create a live engine from multiple root directories with a custom configuration.
    pub fn open_dirs_with_config(paths: &[PathBuf], config: JournalConfig) -> Result<Self> {
        validate_live_config(&config)?;
        let discovery = discover_journal_candidates(paths, &config)?;
        let tracked = build_tracked_files_from_paths(&discovery.candidates, &config)?;
        let live_dedup_capacity = live_dedup_capacity(&config);

        let mut out = Self {
            roots: discovery.roots,
            config,
            tracked_files: tracked.files,
            path_index: tracked.path_index,
            fallback_dirs: Vec::new(),
            subscriptions: Vec::new(),
            last_seen: tracked.last_seen,
            recent_live_entries: HashSet::new(),
            recent_live_order: VecDeque::new(),
            live_dedup_capacity,
            pending_modified_paths: Vec::new(),
            topology_refresh_requested: false,
            next_replay_index: 0,
            prefer_replay_work: false,
            #[cfg(target_os = "linux")]
            inotify: None,
            #[cfg(target_os = "linux")]
            watch_paths: Vec::new(),
        };
        out.refresh_fallback_dirs();
        out.refresh_watchers(false);
        Ok(out)
    }

    /// Create a new live filter builder using this engine's runtime limits.
    ///
    /// Build one filter per subscription. Filters are cheap and do not start watching the journal
    /// until they are registered with [`LiveJournal::subscribe`] or
    /// [`LiveJournal::subscribe_with_options`].
    pub fn filter(&self) -> LiveFilter {
        LiveFilter::new(self.config.clone())
    }

    /// Register a live-only subscription.
    ///
    /// The returned subscription only receives entries after the engine's current tail checkpoint.
    /// It does not scan historical entries. This is the preferred path for normal tailing because
    /// registering the subscription does not rebuild query state.
    pub fn subscribe(&mut self, filter: LiveFilter) -> Result<LiveSubscription> {
        self.subscribe_with_options(SubscriptionOptions::new(filter))
    }

    /// Register a subscription with explicit replay or resume bounds.
    ///
    /// Matching backlog covered by `options` is replayed by subsequent
    /// [`LiveJournal::poll_once`] or [`LiveJournal::run`] calls before future live entries are
    /// dispatched to that subscription.
    /// Replay work is bounded by [`JournalConfig::max_live_batch_entries`] per engine cycle.
    /// [`JournalConfig::max_live_replay_entries`] can add an optional per-subscription total cap.
    ///
    /// This opens an isolated snapshot to establish the replay boundary. Prefer
    /// [`LiveJournal::subscribe`] when only future entries are needed.
    pub fn subscribe_with_options(
        &mut self,
        options: SubscriptionOptions,
    ) -> Result<LiveSubscription> {
        let compiled = options.filter.compile()?;
        let (tx, rx) = mpsc::sync_channel(self.config.live_channel_capacity);
        let alive = Arc::new(AtomicBool::new(true));
        let needs_replay = options.after_cursor.is_some() || options.since_realtime.is_some();

        let (start_after, replay, replay_file_tails) = if needs_replay {
            let (snapshot, replay_file_tails) = self.open_replay_snapshot()?;
            let start_after = snapshot.last_seen;
            let replay = ReplayState::new(
                snapshot,
                options.after_cursor,
                options.since_realtime,
                &self.config,
            );
            (start_after, Some(replay), replay_file_tails)
        } else {
            (self.last_seen, None, HashMap::new())
        };

        self.subscriptions.push(SubscriptionState {
            filter: compiled,
            tx,
            start_after,
            alive: alive.clone(),
            replay,
            replay_file_tails,
            deferred_live: VecDeque::new(),
            terminal_error: None,
        });

        Ok(LiveSubscription { rx, alive })
    }

    /// Poll for live changes once and dispatch any newly appended entries.
    ///
    /// Returns the total number of subscription deliveries performed during this cycle.
    /// This may be larger than the number of journal entries because one entry can match multiple
    /// subscriptions.
    ///
    /// The call blocks for at most [`JournalConfig::poll_interval`] when no platform watcher is
    /// available. On Linux it uses inotify when possible and falls back to polling.
    pub fn poll_once(&mut self) -> Result<usize> {
        self.remove_closed_subscriptions();
        if self.subscriptions.is_empty() {
            return Ok(0);
        }

        if self.prefer_replay_work
            && let Some(deliveries) = self.dispatch_next_replay_batch()?
        {
            self.prefer_replay_work = false;
            self.remove_closed_subscriptions();
            return Ok(deliveries);
        }

        let mut processed_ready_live_work = false;
        if let Some(deliveries) = self.dispatch_ready_live_work()? {
            processed_ready_live_work = true;
            self.prefer_replay_work = true;
            if deliveries != 0 {
                self.remove_closed_subscriptions();
                return Ok(deliveries);
            }
        }

        if let Some(deliveries) = self.dispatch_next_replay_batch()? {
            self.prefer_replay_work = false;
            self.remove_closed_subscriptions();
            return Ok(deliveries);
        }

        if processed_ready_live_work {
            self.remove_closed_subscriptions();
            return Ok(0);
        }

        let change = self.wait_for_change();
        self.dispatch_change(change)
    }

    fn dispatch_ready_live_work(&mut self) -> Result<Option<usize>> {
        if self.topology_refresh_requested {
            self.topology_refresh_requested = false;
            return self.refresh_topology_and_dispatch().map(Some);
        }

        if !self.pending_modified_paths.is_empty() {
            let paths = std::mem::take(&mut self.pending_modified_paths);
            return self.dispatch_modified_paths(&paths).map(Some);
        }

        let change = self.try_collect_ready_change();
        if change.is_empty() {
            return Ok(None);
        }

        self.dispatch_change(change).map(Some)
    }

    fn dispatch_change(&mut self, change: WatchChange) -> Result<usize> {
        if change.is_empty() {
            self.remove_closed_subscriptions();
            return Ok(0);
        }

        if change.topology_changed {
            return self.refresh_topology_and_dispatch();
        }

        self.dispatch_modified_paths(&change.modified_paths)
    }

    /// Run the live engine until every subscription has been dropped.
    ///
    /// This is the simplest way to drive live delivery from a background thread. Dropping all
    /// [`LiveSubscription`] handles causes the loop to exit after the next polling cycle.
    pub fn run(mut self) -> Result<()> {
        while !self.subscriptions.is_empty() {
            let delivered = self.poll_once()?;
            if delivered == 0
                && self
                    .subscriptions
                    .iter()
                    .any(|subscription| subscription.terminal_error.is_some())
            {
                thread::sleep(self.config.poll_interval);
            }
        }
        Ok(())
    }

    fn wait_for_change(&mut self) -> WatchChange {
        core::cfg_select! {
            target_os = "linux" => {
                if let Some(w) = self.inotify.as_mut() {
                    let incomplete = !w.is_complete();
                    let mut change = w.wait(self.config.poll_interval);
                    if incomplete {
                        change.merge(self.scan_all_files());
                        self.refresh_watchers(true);
                    }
                    change
                } else {
                    let change = self.poll_all_files_after_sleep();
                    self.refresh_watchers(true);
                    change
                }
            }
            _ => {
                self.poll_all_files_after_sleep()
            }
        }
    }

    fn try_collect_ready_change(&mut self) -> WatchChange {
        core::cfg_select! {
            target_os = "linux" => {
                if let Some(w) = self.inotify.as_mut() {
                    let incomplete = !w.is_complete();
                    let mut change = w.wait(Duration::ZERO);
                    if incomplete {
                        change.merge(self.scan_all_files());
                    }
                    change
                } else {
                    self.scan_all_files()
                }
            }
            _ => {
                self.scan_all_files()
            }
        }
    }

    fn poll_all_files_after_sleep(&mut self) -> WatchChange {
        thread::sleep(self.config.poll_interval);
        self.scan_all_files()
    }

    fn scan_all_files(&mut self) -> WatchChange {
        let mut topology_changed = false;
        for dir in &self.fallback_dirs {
            match std::fs::metadata(&dir.path).and_then(|meta| meta.modified()) {
                Ok(modified) if Some(modified) != dir.modified => {
                    topology_changed = true;
                    break;
                }
                Err(_) => {
                    topology_changed = true;
                    break;
                }
                _ => {}
            }
        }

        let mut modified_paths = Vec::new();
        for tracked in &self.tracked_files {
            match std::fs::metadata(&tracked.path) {
                Ok(meta) => {
                    let len = meta.len();
                    let known = tracked.live_state.file_len;
                    if len < known {
                        topology_changed = true;
                        break;
                    }
                    if file_metadata_changed(known, tracked.modified, &meta) {
                        modified_paths.push(tracked.path.clone());
                    }
                }
                Err(_) => {
                    topology_changed = true;
                    break;
                }
            }
        }

        WatchChange {
            topology_changed,
            modified_paths,
        }
    }

    fn open_replay_snapshot(&self) -> Result<(JournalSnapshot, HashMap<[u8; 16], u64>)> {
        let discovery = match discover_journal_candidates(&self.roots, &self.config) {
            Ok(discovery) => discovery,
            Err(SdJournalError::NotFound) => {
                return Ok((
                    JournalSnapshot {
                        journal: None,
                        last_seen: None,
                    },
                    HashMap::new(),
                ));
            }
            Err(error) => return Err(error),
        };
        let snapshot = build_live_snapshot(&discovery.candidates, &self.config)?;
        let replay_file_tails = snapshot
            .tracked
            .files
            .iter()
            .filter_map(|tracked| {
                tracked
                    .tail
                    .last_entry_offset()
                    .map(|offset| (tracked.file_id, offset))
            })
            .collect();
        Ok((
            JournalSnapshot {
                journal: Some(snapshot.journal),
                last_seen: snapshot.tracked.last_seen,
            },
            replay_file_tails,
        ))
    }

    fn dispatch_next_replay_batch(&mut self) -> Result<Option<usize>> {
        let Some(idx) = self.next_replay_subscription_index() else {
            return Ok(None);
        };
        self.next_replay_index = idx.saturating_add(1);

        if self.subscriptions[idx].terminal_error.is_some() {
            return Ok(Some(self.dispatch_terminal_error(idx)));
        }

        let Some(replay) = self.subscriptions[idx].replay.as_ref() else {
            return Ok(Some(self.dispatch_deferred_live_batch(idx)));
        };
        if matches!(replay.remaining, Some(0)) {
            let limit = self.config.max_live_replay_entries.unwrap_or(0);
            return Err(SdJournalError::LimitExceeded {
                kind: LimitKind::LiveReplayEntries,
                limit: u64::try_from(limit).unwrap_or(u64::MAX),
            });
        };

        let limit = replay
            .remaining
            .map_or(self.config.max_live_batch_entries, |remaining| {
                self.config.max_live_batch_entries.min(remaining)
            });
        let batch = collect_replay_batch(replay, &self.subscriptions[idx].filter, limit)?;
        let consumed = batch.entries.len();
        let batch_last_key = batch.last_key;
        let batch_exhausted = batch.exhausted;

        let mut delivered = 0usize;
        let mut closed = false;
        let sub = &mut self.subscriptions[idx];
        for entry in batch.entries {
            match send_live_item(
                &sub.tx,
                Ok(LiveEntry::new(entry)),
                self.config.live_queue_full_policy,
            ) {
                SendOutcome::Delivered => delivered = delivered.saturating_add(1),
                SendOutcome::Dropped => {}
                SendOutcome::Closed => {
                    closed = true;
                    break;
                }
            }
        }

        if closed {
            self.subscriptions[idx]
                .alive
                .store(false, AtomicOrdering::Release);
            return Ok(Some(delivered));
        }

        let sub = &mut self.subscriptions[idx];
        if let Some(replay) = sub.replay.as_mut() {
            replay.cursor = None;
            replay.last_key = batch_last_key.or(replay.last_key);
            if let Some(remaining) = replay.remaining.as_mut() {
                *remaining = remaining.saturating_sub(consumed);
            }
        }
        if batch_exhausted {
            sub.replay = None;
            if sub.deferred_live.is_empty() {
                sub.start_after = None;
            }
        }

        Ok(Some(delivered))
    }

    fn dispatch_terminal_error(&mut self, idx: usize) -> usize {
        let Some(error) = self.subscriptions[idx].terminal_error.take() else {
            return 0;
        };
        match self.subscriptions[idx].tx.try_send(Err(error)) {
            Ok(()) => {
                self.subscriptions[idx]
                    .alive
                    .store(false, AtomicOrdering::Release);
                1
            }
            Err(TrySendError::Full(Err(error))) => {
                self.subscriptions[idx].terminal_error = Some(error);
                thread::yield_now();
                0
            }
            Err(TrySendError::Full(Ok(_))) => {
                self.subscriptions[idx].terminal_error = Some(SdJournalError::Transient {
                    path: None,
                    reason: "live terminal-error delivery lost its error payload".to_string(),
                });
                0
            }
            Err(TrySendError::Disconnected(_)) => {
                self.subscriptions[idx]
                    .alive
                    .store(false, AtomicOrdering::Release);
                0
            }
        }
    }

    fn dispatch_deferred_live_batch(&mut self, idx: usize) -> usize {
        let mut delivered = 0usize;
        let mut closed = false;
        let sub = &mut self.subscriptions[idx];
        for _ in 0..self.config.max_live_batch_entries {
            let Some(entry) = sub.deferred_live.pop_front() else {
                break;
            };
            match send_live_item(&sub.tx, Ok(entry), self.config.live_queue_full_policy) {
                SendOutcome::Delivered => delivered = delivered.saturating_add(1),
                SendOutcome::Dropped => {}
                SendOutcome::Closed => {
                    closed = true;
                    break;
                }
            }
        }
        if sub.deferred_live.is_empty() {
            sub.start_after = None;
        }
        if closed {
            sub.alive.store(false, AtomicOrdering::Release);
        }
        delivered
    }

    fn next_replay_subscription_index(&self) -> Option<usize> {
        if self.subscriptions.is_empty() {
            return None;
        }

        let start = self.next_replay_index.min(self.subscriptions.len());
        self.subscriptions[start..]
            .iter()
            .position(|sub| {
                sub.terminal_error.is_some()
                    || sub.replay.is_some()
                    || !sub.deferred_live.is_empty()
            })
            .map(|offset| start + offset)
            .or_else(|| {
                self.subscriptions[..start].iter().position(|sub| {
                    sub.terminal_error.is_some()
                        || sub.replay.is_some()
                        || !sub.deferred_live.is_empty()
                })
            })
    }

    fn refresh_topology_and_dispatch(&mut self) -> Result<usize> {
        let candidates = match discover_journal_candidates(&self.roots, &self.config) {
            Ok(discovery) => discovery.candidates,
            Err(SdJournalError::NotFound) => Vec::new(),
            Err(error) => return Err(error),
        };
        let candidate_paths: HashSet<PathBuf> = candidates.iter().cloned().collect();
        let snapshot = match build_tracked_files_from_paths(&candidates, &self.config) {
            Ok(snapshot) => snapshot,
            Err(error) if is_skippable_live_file_error(&error) => TrackedFiles {
                files: Vec::new(),
                path_index: HashMap::new(),
                last_seen: None,
            },
            Err(error) => return Err(error),
        };
        let represented_paths: HashSet<PathBuf> = snapshot
            .files
            .iter()
            .map(|tracked| tracked.path.clone())
            .collect();
        let mut old_by_identity: HashMap<([u8; 16], [u8; 16]), TrackedFile> = self
            .tracked_files
            .iter()
            .cloned()
            .map(|tracked| ((tracked.file_id, tracked.seqnum_id), tracked))
            .collect();
        let mut rebuilt = Vec::with_capacity(snapshot.files.len());
        let mut sources = Vec::new();
        let mut pending_count = 0usize;
        let mut deferred_paths = Vec::new();

        for template in snapshot.files {
            let identity = (template.file_id, template.seqnum_id);
            let mut tracked = old_by_identity.remove(&identity).unwrap_or_else(|| {
                let mut tracked = template.clone();
                tracked.live_state = crate::file::LiveFileState {
                    used_size: 0,
                    file_len: 0,
                    n_entries: 0,
                    entry_array_offset: 0,
                    tail_object_offset: 0,
                };
                tracked.tail = self::tail::FileTailCursor::at_start();
                tracked.last_key = None;
                tracked
            });
            tracked.path = template.path.clone();
            tracked.modified = template.modified;

            let remaining = self
                .config
                .max_live_batch_entries
                .saturating_sub(pending_count);
            if remaining == 0 {
                deferred_paths.push(tracked.path.clone());
                rebuilt.push(tracked);
                continue;
            }

            let file = JournalFile::open(tracked.path.clone(), &self.config)?;
            if file.file_id() != tracked.file_id || file.seqnum_id() != tracked.seqnum_id {
                return Err(SdJournalError::Transient {
                    path: Some(tracked.path.clone()),
                    reason: "journal file changed identity during topology refresh".to_string(),
                });
            }
            let new_state = file.live_state();
            if new_state.used_size < tracked.live_state.used_size
                || new_state.file_len < tracked.live_state.file_len
                || new_state.n_entries < tracked.live_state.n_entries
            {
                return Err(SdJournalError::Transient {
                    path: Some(tracked.path.clone()),
                    reason: "journal file moved backwards during topology refresh".to_string(),
                });
            }

            let mut next_tail = tracked.tail.clone();
            let batch = next_tail.drain_new_offsets(&file, remaining)?;
            let mut entries = Vec::with_capacity(batch.offsets.len());
            for offset in batch.offsets {
                entries.push(file.read_entry_ref(offset)?);
            }
            pending_count = pending_count.saturating_add(entries.len());
            tracked.last_key = entries.last().map(key_from_entry_ref).or(tracked.last_key);
            tracked.tail = next_tail;
            if batch.exhausted {
                tracked.live_state = new_state;
            } else {
                deferred_paths.push(tracked.path.clone());
            }
            if !entries.is_empty() {
                sources.push(VecDeque::from(entries));
            }
            rebuilt.push(tracked);
        }

        for (_, tracked) in old_by_identity {
            if candidate_paths.contains(&tracked.path) && !represented_paths.contains(&tracked.path)
            {
                deferred_paths.push(tracked.path.clone());
                rebuilt.push(tracked);
            }
        }

        self.tracked_files = rebuilt;
        self.path_index = self
            .tracked_files
            .iter()
            .enumerate()
            .map(|(idx, tracked)| (tracked.path.clone(), idx))
            .collect();
        self.pending_modified_paths.extend(deferred_paths);
        self.pending_modified_paths.sort();
        self.pending_modified_paths.dedup();
        self.refresh_fallback_dirs();
        self.refresh_watchers(true);

        self.dispatch_entries(merge_entry_sources(sources, false), false)
    }

    fn dispatch_modified_paths(&mut self, paths: &[PathBuf]) -> Result<usize> {
        let mut refreshed_files = Vec::new();
        let mut pending_count = 0usize;
        let mut deferred_paths = Vec::new();
        let mut needs_topology_refresh = false;

        for (pos, path) in paths.iter().enumerate() {
            let Some(&idx) = self.path_index.get(path) else {
                if is_candidate_journal_path(path) {
                    needs_topology_refresh = true;
                    break;
                }
                continue;
            };

            let remaining = self
                .config
                .max_live_batch_entries
                .saturating_sub(pending_count);
            if remaining == 0 {
                deferred_paths.extend_from_slice(&paths[pos..]);
                break;
            }
            let Some(refreshed) = self.refresh_tracked_file(idx, remaining)? else {
                needs_topology_refresh = true;
                break;
            };
            pending_count = pending_count.saturating_add(refreshed.entries.len());
            if pending_count >= self.config.max_live_batch_entries {
                deferred_paths.extend_from_slice(&paths[pos + 1..]);
                if !refreshed.exhausted {
                    deferred_paths.push(path.clone());
                }
            }
            refreshed_files.push(refreshed);
            if pending_count >= self.config.max_live_batch_entries {
                break;
            }
        }

        if needs_topology_refresh && pending_count == 0 {
            return self.refresh_topology_and_dispatch();
        }

        let sources = refreshed_files
            .iter_mut()
            .map(|refreshed| VecDeque::from(std::mem::take(&mut refreshed.entries)))
            .collect();
        self.commit_refreshed_files(refreshed_files);
        self.pending_modified_paths.extend(deferred_paths);
        self.pending_modified_paths.sort();
        self.pending_modified_paths.dedup();

        if needs_topology_refresh {
            self.topology_refresh_requested = true;
        }

        // Each source is already ordered by its per-file ENTRY_ARRAY. A pairwise k-way merge is
        // required here because systemd's conditional journal ordering is not a transitive `Ord`.
        self.dispatch_entries(merge_entry_sources(sources, false), false)
    }

    fn refresh_tracked_file(&self, idx: usize, limit: usize) -> Result<Option<RefreshedFile>> {
        let old_state = self.tracked_files[idx].live_state;
        let reopened = match JournalFile::open(self.tracked_files[idx].path.clone(), &self.config) {
            Ok(file) => file,
            Err(err) if is_skippable_live_file_error(&err) => return Ok(None),
            Err(e) => return Err(e),
        };
        let modified = reopened.modified_at_open();

        let tracked = &self.tracked_files[idx];
        if reopened.file_id() != tracked.file_id || reopened.seqnum_id() != tracked.seqnum_id {
            return Ok(None);
        }

        let new_state = reopened.live_state();
        if new_state.used_size < old_state.used_size
            || new_state.file_len < old_state.file_len
            || new_state.n_entries < old_state.n_entries
        {
            return Ok(None);
        }
        if new_state == old_state {
            return Ok(Some(RefreshedFile {
                tracked_idx: idx,
                next_tail: tracked.tail.clone(),
                next_live_state: new_state,
                next_last_key: tracked.last_key,
                modified,
                entries: Vec::new(),
                exhausted: true,
            }));
        }

        let mut next_tail = tracked.tail.clone();
        let batch = match next_tail.drain_new_offsets(&reopened, limit) {
            Ok(batch) => batch,
            Err(SdJournalError::Transient { .. }) | Err(SdJournalError::Corrupt { .. }) => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        let mut entries = Vec::with_capacity(batch.offsets.len());
        for offset in batch.offsets {
            match reopened.read_entry_ref(offset) {
                Ok(entry) => entries.push(entry),
                Err(err) if is_skippable_live_file_error(&err) => {
                    warn_live_file_error("skipping corrupt live entry", &err);
                    return Ok(None);
                }
                Err(err) => return Err(err),
            }
        }
        let next_last_key = entries.last().map(key_from_entry_ref).or(tracked.last_key);
        Ok(Some(RefreshedFile {
            tracked_idx: idx,
            next_tail,
            next_live_state: if batch.exhausted {
                new_state
            } else {
                old_state
            },
            next_last_key,
            modified,
            entries,
            exhausted: batch.exhausted,
        }))
    }

    fn commit_refreshed_files(&mut self, refreshed_files: Vec<RefreshedFile>) {
        for refreshed in refreshed_files {
            let tracked = &mut self.tracked_files[refreshed.tracked_idx];
            tracked.tail = refreshed.next_tail;
            tracked.live_state = refreshed.next_live_state;
            tracked.last_key = refreshed.next_last_key;
            tracked.modified = refreshed.modified;
        }
    }

    fn dispatch_entries(
        &mut self,
        pending: Vec<EntryRef>,
        enforce_start_after: bool,
    ) -> Result<usize> {
        if pending.is_empty() {
            self.remove_closed_subscriptions();
            return Ok(0);
        }

        let mut deliveries = 0usize;
        let mut dead = vec![false; self.subscriptions.len()];
        let mut matched = Vec::with_capacity(self.subscriptions.len());

        for owned in pending {
            let key = key_from_entry_ref(&owned);
            if !self.remember_live_entry(key) {
                continue;
            }
            matched.clear();

            for (idx, sub) in self.subscriptions.iter_mut().enumerate() {
                if dead[idx] || sub.terminal_error.is_some() {
                    continue;
                }

                let defer = sub.replay.is_some() || !sub.deferred_live.is_empty();
                if let Some(tail_offset) = sub.replay_file_tails.get(&key.file_id).copied() {
                    if key.entry_offset <= tail_offset {
                        continue;
                    }
                    sub.replay_file_tails.remove(&key.file_id);
                }
                if enforce_start_after && let Some(start_after) = sub.start_after {
                    if compare_keys(&key, &start_after) != Ordering::Greater {
                        continue;
                    }
                    if !defer {
                        sub.start_after = None;
                    }
                }

                if sub.filter.matches(&owned) {
                    matched.push((idx, defer));
                }
            }

            if !matched.is_empty() {
                let shared = LiveEntry::new(owned);
                for (idx, defer) in matched.iter().copied() {
                    if defer {
                        let sub = &mut self.subscriptions[idx];
                        if sub.deferred_live.len() < self.config.live_channel_capacity {
                            sub.deferred_live.push_back(shared.clone());
                            continue;
                        }
                        match self.config.live_queue_full_policy {
                            LiveQueueFullPolicy::DropNewest => continue,
                            LiveQueueFullPolicy::Disconnect => {
                                dead[idx] = true;
                                continue;
                            }
                            LiveQueueFullPolicy::Block => {
                                sub.replay = None;
                                sub.deferred_live.clear();
                                sub.terminal_error = Some(SdJournalError::Transient {
                                    path: None,
                                    reason: format!(
                                        "live replay catch-up buffer exceeded {} entries",
                                        self.config.live_channel_capacity
                                    ),
                                });
                                continue;
                            }
                        }
                    }
                    match send_live_item(
                        &self.subscriptions[idx].tx,
                        Ok(shared.clone()),
                        self.config.live_queue_full_policy,
                    ) {
                        SendOutcome::Delivered => deliveries = deliveries.saturating_add(1),
                        SendOutcome::Dropped => {}
                        SendOutcome::Closed => dead[idx] = true,
                    }
                }
            }

            self.advance_last_seen(Some(key));
        }

        if dead.iter().any(|dead| *dead) {
            for (idx, is_dead) in dead.iter().copied().enumerate() {
                if is_dead {
                    self.subscriptions[idx]
                        .alive
                        .store(false, AtomicOrdering::Release);
                }
            }
            let mut idx = 0usize;
            self.subscriptions.retain(|_| {
                let keep = !dead[idx];
                idx = idx.saturating_add(1);
                keep
            });
            self.next_replay_index = self.next_replay_index.min(self.subscriptions.len());
        }

        Ok(deliveries)
    }

    fn refresh_watchers(&mut self, force: bool) {
        #[cfg(target_os = "linux")]
        {
            let watch_paths = collect_watch_paths(&self.roots, &self.tracked_files);
            let incomplete = self
                .inotify
                .as_ref()
                .is_none_or(|watcher| !watcher.is_complete());
            if force || incomplete || watch_paths != self.watch_paths {
                self.inotify = InotifyWatcher::new(&watch_paths);
                self.watch_paths = watch_paths;
                self.queue_tracked_paths_for_recheck();
                #[cfg(feature = "tracing")]
                debug!(
                    inotify = self.inotify.is_some(),
                    n_watch_paths = self.watch_paths.len(),
                    "live watcher refreshed"
                );
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = force;
    }

    fn refresh_fallback_dirs(&mut self) {
        self.fallback_dirs = collect_fallback_dirs(&self.roots, &self.tracked_files);
    }

    fn queue_tracked_paths_for_recheck(&mut self) {
        self.pending_modified_paths.extend(
            self.tracked_files
                .iter()
                .map(|tracked| tracked.path.clone()),
        );
        self.pending_modified_paths.sort();
        self.pending_modified_paths.dedup();
    }

    fn remove_closed_subscriptions(&mut self) {
        self.subscriptions
            .retain(|sub| sub.alive.load(AtomicOrdering::Acquire));
        self.next_replay_index = self.next_replay_index.min(self.subscriptions.len());
    }

    fn advance_last_seen(&mut self, key: Option<SdJournalEntryKey>) {
        let Some(key) = key else {
            return;
        };
        if self
            .last_seen
            .as_ref()
            .is_none_or(|last| compare_keys(&key, last) == Ordering::Greater)
        {
            self.last_seen = Some(key);
        }
    }

    fn remember_live_entry(&mut self, key: SdJournalEntryKey) -> bool {
        let logical = logical_entry_identity(key);
        if !self.recent_live_entries.insert(logical) {
            return false;
        }
        self.recent_live_order.push_back(logical);
        while self.recent_live_order.len() > self.live_dedup_capacity {
            if let Some(expired) = self.recent_live_order.pop_front() {
                self.recent_live_entries.remove(&expired);
            }
        }
        true
    }
}

/// Options for `LiveJournal` subscriptions.
///
/// Without additional bounds, subscriptions are live-only and start after the current tail.
/// Adding [`SubscriptionOptions::after_cursor`] or [`SubscriptionOptions::since_realtime`]
/// requests a replay before the subscription switches to live delivery.
#[derive(Clone)]
pub struct SubscriptionOptions {
    filter: LiveFilter,
    after_cursor: Option<Cursor>,
    since_realtime: Option<u64>,
}

impl SubscriptionOptions {
    /// Create subscription options from a live filter.
    pub fn new(filter: LiveFilter) -> Self {
        Self {
            filter,
            after_cursor: None,
            since_realtime: None,
        }
    }

    /// Replay matching entries strictly after `cursor` before switching to live delivery.
    ///
    /// This is intended for checkpoint-based consumers that persist [`crate::Cursor`] values.
    pub fn after_cursor(&mut self, cursor: Cursor) -> &mut Self {
        self.after_cursor = Some(cursor);
        self
    }

    /// Replay matching entries from `usec` before switching to live delivery.
    ///
    /// `usec` is a realtime timestamp in microseconds since the Unix epoch.
    pub fn since_realtime(&mut self, usec: u64) -> &mut Self {
        self.since_realtime = Some(usec);
        self
    }
}

/// Receiving end of a live subscription.
///
/// Values are delivered as shared [`LiveEntry`] handles so one decoded entry can be fanned out to
/// multiple subscribers without duplicating field storage.
/// Each subscription has a bounded queue controlled by [`JournalConfig::live_channel_capacity`].
///
/// Dropping a subscription unregisters it from the engine. When all subscriptions are dropped,
/// [`LiveJournal::run`] exits.
pub struct LiveSubscription {
    rx: Receiver<Result<LiveEntry>>,
    alive: Arc<AtomicBool>,
}

impl LiveSubscription {
    /// Receive the next entry, blocking until the subscription closes or a value arrives.
    ///
    /// The outer `Result` reports channel closure. The inner crate [`Result`] reports journal
    /// decoding or I/O errors produced by the live engine.
    pub fn recv(&self) -> std::result::Result<Result<LiveEntry>, mpsc::RecvError> {
        self.rx.recv()
    }

    /// Receive the next entry, waiting at most `timeout`.
    ///
    /// The outer `Result` distinguishes timeout or channel closure from a delivered value. The
    /// delivered value is still a crate [`Result`] because reading the journal can fail.
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> std::result::Result<Result<LiveEntry>, mpsc::RecvTimeoutError> {
        self.rx.recv_timeout(timeout)
    }

    /// Try to receive the next queued entry without blocking.
    ///
    /// This is useful when integrating [`LiveJournal::poll_once`] into an existing event loop.
    pub fn try_recv(&self) -> std::result::Result<Result<LiveEntry>, mpsc::TryRecvError> {
        self.rx.try_recv()
    }
}

impl Drop for LiveSubscription {
    fn drop(&mut self) {
        self.alive.store(false, AtomicOrdering::Release);
    }
}

fn validate_live_config(config: &JournalConfig) -> Result<()> {
    if config.max_open_files == 0 {
        return Err(SdJournalError::InvalidQuery {
            reason: "max_open_files must be greater than zero".to_string(),
        });
    }
    if config.live_channel_capacity == 0 {
        return Err(SdJournalError::InvalidQuery {
            reason: "live_channel_capacity must be greater than zero".to_string(),
        });
    }
    if config.max_live_batch_entries == 0 {
        return Err(SdJournalError::InvalidQuery {
            reason: "max_live_batch_entries must be greater than zero".to_string(),
        });
    }
    if config.max_live_replay_entries == Some(0) {
        return Err(SdJournalError::InvalidQuery {
            reason: "max_live_replay_entries must be greater than zero".to_string(),
        });
    }
    if config.live_queue_full_policy == LiveQueueFullPolicy::Block
        && config.max_live_batch_entries > config.live_channel_capacity
    {
        return Err(SdJournalError::InvalidQuery {
            reason: "max_live_batch_entries must not exceed live_channel_capacity when live_queue_full_policy is Block".to_string(),
        });
    }
    Ok(())
}

fn send_live_item(
    tx: &SyncSender<Result<LiveEntry>>,
    item: Result<LiveEntry>,
    policy: LiveQueueFullPolicy,
) -> SendOutcome {
    match policy {
        LiveQueueFullPolicy::Block => match tx.send(item) {
            Ok(()) => SendOutcome::Delivered,
            Err(_) => SendOutcome::Closed,
        },
        LiveQueueFullPolicy::DropNewest => match tx.try_send(item) {
            Ok(()) => SendOutcome::Delivered,
            Err(TrySendError::Full(_)) => SendOutcome::Dropped,
            Err(TrySendError::Disconnected(_)) => SendOutcome::Closed,
        },
        LiveQueueFullPolicy::Disconnect => match tx.try_send(item) {
            Ok(()) => SendOutcome::Delivered,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => SendOutcome::Closed,
        },
    }
}

fn is_skippable_live_file_error(err: &SdJournalError) -> bool {
    matches!(
        err,
        SdJournalError::Retired { .. }
            | SdJournalError::Corrupt { .. }
            | SdJournalError::Transient { .. }
            | SdJournalError::Io { .. }
            | SdJournalError::LimitExceeded {
                kind: LimitKind::ObjectChainSteps,
                ..
            }
    )
}

fn warn_skipped_live_file(path: &std::path::Path, err: &SdJournalError) {
    #[cfg(feature = "tracing")]
    warn!(
        path = %path.display(),
        error = %err,
        "skipping journal file for live tailing"
    );

    let _ = (path, err);
}

fn warn_live_file_error(message: &'static str, err: &SdJournalError) {
    #[cfg(feature = "tracing")]
    warn!(error = %err, "{message}");

    let _ = (message, err);
}

fn is_candidate_journal_path(path: &std::path::Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("journal") | Some("journal~")
    )
}

fn key_from_entry_ref(entry: &EntryRef) -> SdJournalEntryKey {
    entry.entry_key()
}

fn cursor_from_key(key: SdJournalEntryKey) -> Cursor {
    Cursor::new_location_key(key)
}

fn compare_keys(left: &SdJournalEntryKey, right: &SdJournalEntryKey) -> Ordering {
    compare_entry_keys(left, right)
}

fn logical_entry_identity(key: SdJournalEntryKey) -> LogicalEntryId {
    LogicalEntryId {
        seqnum_id: key.seqnum_id,
        seqnum: key.seqnum,
        boot_id: key.boot_id,
        monotonic_usec: key.monotonic_usec,
        realtime_usec: key.realtime_usec,
        xor_hash: key.xor_hash,
    }
}

fn live_dedup_capacity(config: &JournalConfig) -> usize {
    config
        .max_journal_files
        .saturating_add(config.max_live_batch_entries)
        .max(config.live_channel_capacity)
        .clamp(1, 65_536)
}

fn merge_entry_sources(mut sources: Vec<VecDeque<EntryRef>>, reverse: bool) -> Vec<EntryRef> {
    let capacity = sources
        .iter()
        .fold(0usize, |total, source| total.saturating_add(source.len()));
    let mut merged = Vec::with_capacity(capacity);

    loop {
        let mut selected: Option<usize> = None;
        for (idx, source) in sources.iter().enumerate() {
            let Some(candidate) = source.front() else {
                continue;
            };
            let Some(best_idx) = selected else {
                selected = Some(idx);
                continue;
            };
            let Some(best) = sources[best_idx].front() else {
                continue;
            };
            let ordering = compare_keys(&key_from_entry_ref(candidate), &key_from_entry_ref(best));
            let replaces = if reverse {
                ordering == Ordering::Greater || (ordering == Ordering::Equal && idx < best_idx)
            } else {
                ordering == Ordering::Less || (ordering == Ordering::Equal && idx < best_idx)
            };
            if replaces {
                selected = Some(idx);
            }
        }

        let Some(selected) = selected else {
            break;
        };
        let Some(entry) = sources[selected].pop_front() else {
            break;
        };
        merged.push(entry);
    }

    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_keys_keeps_sequence_order_across_realtime_rollback() {
        let earlier = SdJournalEntryKey {
            file_id: [0x11; 16],
            entry_offset: 1,
            seqnum_id: [0x33; 16],
            seqnum: 2,
            boot_id: [0x44; 16],
            monotonic_usec: 20,
            realtime_usec: 100,
            xor_hash: 7,
        };
        let later = SdJournalEntryKey {
            file_id: [0x22; 16],
            entry_offset: 4,
            seqnum_id: [0x33; 16],
            seqnum: 5,
            boot_id: [0x44; 16],
            monotonic_usec: 30,
            realtime_usec: 50,
            xor_hash: 8,
        };

        assert_eq!(compare_keys(&earlier, &later), Ordering::Less);
        assert_eq!(compare_keys(&later, &earlier), Ordering::Greater);
        assert_eq!(compare_keys(&earlier, &earlier), Ordering::Equal);
    }

    #[test]
    fn validate_live_config_rejects_unbounded_or_blocking_unsafe_values() {
        let mut cfg = JournalConfig {
            live_channel_capacity: 0,
            ..Default::default()
        };
        assert!(matches!(
            validate_live_config(&cfg),
            Err(SdJournalError::InvalidQuery { .. })
        ));

        cfg = JournalConfig {
            max_open_files: 0,
            ..Default::default()
        };
        assert!(matches!(
            validate_live_config(&cfg),
            Err(SdJournalError::InvalidQuery { .. })
        ));

        cfg = JournalConfig {
            max_live_batch_entries: 0,
            ..Default::default()
        };
        assert!(matches!(
            validate_live_config(&cfg),
            Err(SdJournalError::InvalidQuery { .. })
        ));

        cfg = JournalConfig {
            max_live_replay_entries: Some(0),
            ..Default::default()
        };
        assert!(matches!(
            validate_live_config(&cfg),
            Err(SdJournalError::InvalidQuery { .. })
        ));

        cfg = JournalConfig {
            live_channel_capacity: 1,
            max_live_batch_entries: 2,
            live_queue_full_policy: LiveQueueFullPolicy::Block,
            ..Default::default()
        };
        assert!(matches!(
            validate_live_config(&cfg),
            Err(SdJournalError::InvalidQuery { .. })
        ));

        cfg.live_queue_full_policy = LiveQueueFullPolicy::Disconnect;
        assert!(validate_live_config(&cfg).is_ok());
    }
}
