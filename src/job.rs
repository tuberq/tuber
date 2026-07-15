use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

pub const MAX_TUBE_NAME_LEN: usize = 200;
pub const URGENT_THRESHOLD: u32 = 1024;
pub const JOB_DATA_SIZE_LIMIT_DEFAULT: u32 = 65535;
pub const JOB_DATA_SIZE_LIMIT_MAX: u32 = 1_073_741_824;

/// Logical identifier for a body stored in the external body store
/// (`BodyStore`). Monotonic and never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BodyId(pub u64);

/// Inline-body upper bound for the zero-heap-alloc `Tiny` variant.
/// Sized to fit a binary UUID (16B) and a `u64` decimal (≤20B) with
/// room to spare, while keeping the enum slot the same width as the
/// 24-byte `Vec<u8>`/`BodyId` payload it sits next to (23 bytes data +
/// 1 byte length).
pub const TINY_INLINE_BYTES: usize = 23;

/// Upper bound for inline-in-WAL bodies. Above this, bodies go through
/// the external body store so the per-body TOAST overhead amortises.
/// Below this they ride along inside the WAL FullJob record.
pub const HEAP_INLINE_MAX: usize = 256;

/// Single source of truth for the put-time / size-estimate placement
/// decision: a body goes to the external body store iff the store is
/// configured and the body is larger than the inline threshold.
pub fn should_externalize(body_len: usize, body_store_enabled: bool) -> bool {
    body_store_enabled && body_len > HEAP_INLINE_MAX
}

/// Where a job's body lives.
///
/// `Tiny` keeps very small bodies inside the enum slot itself (no heap
/// allocation). `Heap` keeps medium bodies on the heap but still inline
/// in the WAL record (no body-store traffic). `External` references a
/// body owned by the `BodyStore` (TOAST).
#[derive(Debug, Clone)]
pub enum BodyRef {
    Tiny { len: u8, bytes: [u8; TINY_INLINE_BYTES] },
    Heap(Vec<u8>),
    External(BodyId),
}

impl BodyRef {
    /// Wrap owned bytes in the cheapest inline tier. Bytes that fit in
    /// `TINY_INLINE_BYTES` go into the enum slot; larger ones stay on
    /// the heap. Never produces `External` — that decision lives at the
    /// put path where the body-store is available.
    pub fn new_inline(bytes: Vec<u8>) -> Self {
        if bytes.len() <= TINY_INLINE_BYTES {
            let mut buf = [0u8; TINY_INLINE_BYTES];
            buf[..bytes.len()].copy_from_slice(&bytes);
            BodyRef::Tiny { len: bytes.len() as u8, bytes: buf }
        } else {
            BodyRef::Heap(bytes)
        }
    }

    /// Number of body bytes resident in RAM. `External` returns 0
    /// because those bytes live in the body store, not this `Job`.
    /// Drives the in-memory budget accounting.
    pub fn len(&self) -> usize {
        match self {
            BodyRef::Tiny { len, .. } => *len as usize,
            BodyRef::Heap(v) => v.len(),
            BodyRef::External(_) => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrow inline bytes if the body is `Tiny` or `Heap`. Returns
    /// `None` for `External` (where bytes live in the body store).
    pub fn as_inline_bytes(&self) -> Option<&[u8]> {
        match self {
            BodyRef::Tiny { len, bytes } => Some(&bytes[..*len as usize]),
            BodyRef::Heap(v) => Some(v.as_slice()),
            BodyRef::External(_) => None,
        }
    }

    /// If this body is inline (`Tiny` or `Heap`), take its bytes and
    /// leave an empty placeholder. Returns `None` for `External`. Used
    /// by the legacy WAL replay migration path.
    pub fn take_inline(&mut self) -> Option<Vec<u8>> {
        match self {
            BodyRef::Tiny { len, bytes } => {
                let v = bytes[..*len as usize].to_vec();
                *len = 0;
                Some(v)
            }
            BodyRef::Heap(_) => {
                let placeholder = BodyRef::Heap(Vec::new());
                let BodyRef::Heap(bytes) = std::mem::replace(self, placeholder) else {
                    unreachable!()
                };
                Some(bytes)
            }
            BodyRef::External(_) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Ready,
    Reserved,
    Delayed,
    Buried,
}

impl JobState {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobState::Ready => "ready",
            JobState::Reserved => "reserved",
            JobState::Delayed => "delayed",
            JobState::Buried => "buried",
        }
    }

    pub fn as_protocol_str(&self) -> &'static str {
        match self {
            JobState::Ready => "READY",
            JobState::Reserved => "RESERVED",
            JobState::Delayed => "DELAYED",
            JobState::Buried => "BURIED",
        }
    }
}

/// Extension keys carried by `idp:`/`grp:`/`aft:`/`con:` put tags. Most
/// jobs have none, so `Job` holds these behind `Option<Box<JobExt>>` —
/// see the field comment there. Allocate only when a tag is present.
#[derive(Debug, Clone, Default)]
pub struct JobExt {
    pub idempotency_key: Option<(String, u32)>,
    pub group: Option<String>,
    pub after_group: Option<String>,
    pub concurrency_key: Option<(String, u32)>,
}

impl JobExt {
    /// True when every key is absent — such an ext should not be stored.
    pub fn is_empty(&self) -> bool {
        self.idempotency_key.is_none()
            && self.group.is_none()
            && self.after_group.is_none()
            && self.concurrency_key.is_none()
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: u64,
    pub priority: u32,
    pub delay: Duration,
    pub ttr: Duration,
    pub body: BodyRef,
    pub state: JobState,
    /// Shared with the owning `Tube` (and every sibling job): one
    /// allocation per tube name, not per job. Clones are a refcount
    /// bump. Derefs to `&str`, so reads look like the `String` it
    /// replaced.
    pub tube_name: Arc<str>,

    // Future feature extension fields
    /// Rarely-used extension keys (`idp:`/`grp:`/`aft:`/`con:` put tags),
    /// boxed so the common untagged job pays 8 bytes instead of 112.
    /// `Job` lives by value in the server's job table, so its inline size
    /// multiplies across every slot (including empty ones); keep cold,
    /// mostly-absent fields behind a pointer.
    pub ext: Option<Box<JobExt>>,

    pub created_at: Instant,
    /// For delayed jobs: when it becomes ready.
    /// For reserved jobs: when TTR expires.
    pub deadline_at: Option<Instant>,

    /// Connection ID of the reserver, if reserved.
    pub reserver_id: Option<u64>,

    /// When the job was last reserved (for processing time tracking).
    pub reserved_at: Option<Instant>,

    // Counters
    pub reserve_ct: u32,
    pub timeout_ct: u32,
    pub release_ct: u32,
    pub bury_ct: u32,
    pub kick_ct: u32,

    // WAL persistence fields. Never serialized — rebuilt on every
    // replay — so these can be as narrow as the values demand: segment
    // seqs start at 1 (NonZeroU32 keeps Option free), and the largest
    // possible record is a legacy v3/v4 FullJob inlining a body capped
    // at JOB_DATA_SIZE_LIMIT_MAX (1 GiB) — v7 bodies over 256 B live in
    // TOAST, referenced by id — so u32 is ample for both.
    pub wal_file_seq: Option<std::num::NonZeroU32>,
    pub wal_used: u32,
    pub created_at_epoch: u64,
}

impl Job {
    pub fn new(
        id: u64,
        priority: u32,
        delay: Duration,
        ttr: Duration,
        body: Vec<u8>,
        tube_name: impl Into<Arc<str>>,
    ) -> Self {
        let now = Instant::now();
        let (state, deadline_at) = if delay.is_zero() {
            (JobState::Ready, None)
        } else {
            (JobState::Delayed, Some(now + delay))
        };

        Job {
            id,
            priority,
            delay,
            ttr,
            body: BodyRef::new_inline(body),
            state,
            tube_name: tube_name.into(),
            ext: None,
            created_at: now,
            deadline_at,
            reserver_id: None,
            reserved_at: None,
            reserve_ct: 0,
            timeout_ct: 0,
            release_ct: 0,
            bury_ct: 0,
            kick_ct: 0,
            wal_file_seq: None,
            wal_used: 0,
            created_at_epoch: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Returns the priority sort key for the ready heap: (priority, id).
    /// Lower priority number = higher urgency. Ties broken by lower id (FIFO).
    pub fn ready_key(&self) -> (u32, u64) {
        (self.priority, self.id)
    }

    // --- extension-key accessors (see the `ext` field comment) ---

    pub fn idempotency_key(&self) -> Option<&(String, u32)> {
        self.ext.as_ref().and_then(|e| e.idempotency_key.as_ref())
    }

    pub fn group(&self) -> Option<&String> {
        self.ext.as_ref().and_then(|e| e.group.as_ref())
    }

    pub fn after_group(&self) -> Option<&String> {
        self.ext.as_ref().and_then(|e| e.after_group.as_ref())
    }

    pub fn concurrency_key(&self) -> Option<&(String, u32)> {
        self.ext.as_ref().and_then(|e| e.concurrency_key.as_ref())
    }

    /// Mutable access to the extension keys, allocating the box on first
    /// use. Callers that may end up storing nothing should prefer
    /// `set_ext`, which never leaves an all-`None` allocation behind.
    pub fn ext_mut(&mut self) -> &mut JobExt {
        self.ext.get_or_insert_with(Default::default)
    }

    /// Store `ext` unless it is empty (all keys `None`).
    pub fn set_ext(&mut self, ext: JobExt) {
        self.ext = if ext.is_empty() {
            None
        } else {
            Some(Box::new(ext))
        };
    }

    // --- WAL tracking accessors (see the field comment) ---

    /// Record where this job's FullJob record lives in the WAL.
    pub fn set_wal_ref(&mut self, seq: u64, used: usize) {
        debug_assert!(seq <= u32::MAX as u64, "WAL segment seq overflows u32");
        debug_assert!(used <= u32::MAX as usize, "WAL record size overflows u32");
        self.wal_file_seq = std::num::NonZeroU32::new(seq as u32);
        self.wal_used = used as u32;
    }

    pub fn clear_wal_ref(&mut self) {
        self.wal_file_seq = None;
        self.wal_used = 0;
    }

    /// Segment seq as the WAL's native width.
    pub fn wal_seq(&self) -> Option<u64> {
        self.wal_file_seq.map(|s| s.get() as u64)
    }

    pub fn is_urgent(&self) -> bool {
        self.priority < URGENT_THRESHOLD
    }

    pub fn body_size(&self) -> usize {
        self.body.len()
    }
}

/// Collect every `BodyId` referenced by an External body in `jobs`. Used
/// by both the WAL replay's orphan filter (re-creates within the same
/// WAL invalidate orphan candidates) and the startup integrity pass
/// (TOAST bodies without a WAL anchor).
pub fn live_external_body_ids<I>(jobs: I) -> std::collections::HashSet<BodyId>
where
    I: IntoIterator,
    I::Item: std::borrow::Borrow<Job>,
{
    use std::borrow::Borrow;
    jobs.into_iter()
        .filter_map(|j| match &j.borrow().body {
            BodyRef::External(id) => Some(*id),
            BodyRef::Tiny { .. } | BodyRef::Heap(_) => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_job(id: u64, pri: u32) -> Job {
        Job::new(
            id,
            pri,
            Duration::ZERO,
            Duration::from_secs(1),
            vec![],
            "default",
        )
    }

    // Mirrors cttest_job_creation
    #[test]
    fn test_job_creation() {
        let j = make_test_job(1, 10);
        assert_eq!(j.id, 1);
        assert_eq!(j.priority, 10);
        assert_eq!(j.state, JobState::Ready);
        assert_eq!(j.tube_name.as_ref(), "default");
    }

    // Mirrors cttest_job_cmp_pris
    #[test]
    fn test_job_cmp_priorities() {
        let j1 = make_test_job(1, 1);
        let j2 = make_test_job(2, 2);
        // Lower priority number is "less" (higher urgency)
        assert!(j1.ready_key() < j2.ready_key());
    }

    // Mirrors cttest_job_cmp_ids
    #[test]
    fn test_job_cmp_ids() {
        let j1 = make_test_job(1, 1);
        let j2 = make_test_job(2, 1);
        // Same priority, lower ID wins
        assert!(j1.ready_key() < j2.ready_key());
    }

    // Mirrors cttest_job_large_pris
    #[test]
    fn test_job_large_priorities() {
        let j1 = make_test_job(1, 0);
        let j2 = make_test_job(2, u32::MAX);
        assert!(j1.ready_key() < j2.ready_key());
    }

    // Mirrors cttest_job_hash_free -- in Rust this is just HashMap remove
    #[test]
    fn test_job_hash_free() {
        use std::collections::HashMap;
        let mut jobs: HashMap<u64, Job> = HashMap::new();
        let j = make_test_job(1, 1);
        jobs.insert(j.id, j);
        assert_eq!(jobs.len(), 1);
        jobs.remove(&1);
        assert_eq!(jobs.len(), 0);
    }

    // Mirrors cttest_job_hash_free_next
    #[test]
    fn test_job_hash_free_chain() {
        use std::collections::HashMap;
        let mut jobs: HashMap<u64, Job> = HashMap::new();
        let j1 = make_test_job(1, 1);
        let j2 = make_test_job(2, 1);
        jobs.insert(j1.id, j1);
        jobs.insert(j2.id, j2);
        assert_eq!(jobs.len(), 2);

        jobs.remove(&1);
        assert_eq!(jobs.len(), 1);
        assert!(jobs.contains_key(&2));

        jobs.remove(&2);
        assert_eq!(jobs.len(), 0);
    }

    // Mirrors cttest_job_all_jobs_used
    #[test]
    fn test_job_count() {
        use std::collections::HashMap;
        let mut jobs: HashMap<u64, Job> = HashMap::new();
        for i in 1..=5 {
            jobs.insert(i, make_test_job(i, 1));
        }
        assert_eq!(jobs.len(), 5);
        jobs.remove(&3);
        assert_eq!(jobs.len(), 4);
    }

    // Mirrors cttest_job_100_000_jobs
    #[test]
    fn test_job_100k_jobs() {
        use std::collections::HashMap;
        let mut jobs: HashMap<u64, Job> = HashMap::new();
        let n = 100_000u64;
        for i in 1..=n {
            jobs.insert(i, make_test_job(i, 1));
        }
        assert_eq!(jobs.len(), n as usize);

        for i in 1..=n {
            jobs.remove(&i);
        }
        assert_eq!(jobs.len(), 0);
    }

    #[test]
    fn test_job_state_str() {
        assert_eq!(JobState::Ready.as_str(), "ready");
        assert_eq!(JobState::Reserved.as_str(), "reserved");
        assert_eq!(JobState::Delayed.as_str(), "delayed");
        assert_eq!(JobState::Buried.as_str(), "buried");
    }

    #[test]
    fn test_job_reserved_at_none_on_creation() {
        let j = make_test_job(1, 10);
        assert!(j.reserved_at.is_none());
    }

    #[test]
    fn test_body_ref_new_inline_picks_tier() {
        // ≤ TINY_INLINE_BYTES → Tiny, no heap allocation.
        let tiny = BodyRef::new_inline(b"abc".to_vec());
        assert!(matches!(tiny, BodyRef::Tiny { len: 3, .. }));
        assert_eq!(tiny.len(), 3);
        assert_eq!(tiny.as_inline_bytes(), Some(&b"abc"[..]));

        // exactly TINY_INLINE_BYTES → still Tiny.
        let edge = BodyRef::new_inline(vec![7u8; TINY_INLINE_BYTES]);
        assert!(matches!(edge, BodyRef::Tiny { .. }));
        assert_eq!(edge.len(), TINY_INLINE_BYTES);

        // TINY_INLINE_BYTES + 1 → Heap.
        let heap = BodyRef::new_inline(vec![8u8; TINY_INLINE_BYTES + 1]);
        assert!(matches!(heap, BodyRef::Heap(_)));
        assert_eq!(heap.len(), TINY_INLINE_BYTES + 1);
    }

    #[test]
    fn test_body_ref_job_new_uses_tier() {
        // 11-byte body fits in Tiny.
        let job = Job::new(
            1,
            10,
            Duration::ZERO,
            Duration::from_secs(1),
            b"hello-toast".to_vec(),
            "default",
        );
        assert!(matches!(job.body, BodyRef::Tiny { len: 11, .. }));
        assert_eq!(job.body_size(), 11);
        assert_eq!(job.body.as_inline_bytes(), Some(&b"hello-toast"[..]));

        // 64-byte body spills to Heap.
        let big = Job::new(
            2,
            10,
            Duration::ZERO,
            Duration::from_secs(1),
            vec![1u8; 64],
            "default",
        );
        assert!(matches!(big.body, BodyRef::Heap(_)));
        assert_eq!(big.body_size(), 64);
    }

    #[test]
    fn test_body_ref_take_inline_tiny() {
        let mut b = BodyRef::new_inline(b"abc".to_vec());
        assert!(matches!(b, BodyRef::Tiny { len: 3, .. }));
        assert_eq!(b.take_inline().as_deref(), Some(&b"abc"[..]));
        assert!(b.is_empty());
    }

    #[test]
    fn test_body_ref_take_inline_heap() {
        let mut b = BodyRef::Heap(b"a much longer body that won't fit in Tiny".to_vec());
        assert_eq!(
            b.take_inline().as_deref(),
            Some(&b"a much longer body that won't fit in Tiny"[..]),
        );
        assert!(matches!(b, BodyRef::Heap(ref v) if v.is_empty()));
    }

    #[test]
    fn test_body_ref_take_inline_external_returns_none() {
        let mut external = BodyRef::External(BodyId(7));
        assert!(external.take_inline().is_none());
        assert!(matches!(external, BodyRef::External(BodyId(7))));
    }

    #[test]
    fn test_job_urgent() {
        let urgent = make_test_job(1, 0);
        assert!(urgent.is_urgent());

        let urgent2 = make_test_job(2, 1023);
        assert!(urgent2.is_urgent());

        let not_urgent = make_test_job(3, 1024);
        assert!(!not_urgent.is_urgent());
    }
}
