//! Opt-in, process-local submission diagnostics. No wire or durable representation.
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

/// Maximum completed attempt reports retained per gateway; full queues drop reports.
pub const REPORT_CAPACITY: usize = 64;
/// Fixed phase slots. Parent spans include children; never sum them as wall time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum Phase {
    /// Waiting for the gateway's existing submission mutex.
    Queue,
    /// Canonical command encoding and external ID validation.
    Canonical,
    /// Lookup and validation of the durable original request.
    Recovery,
    /// Cohort snapshot acquisition, including blocking materialization.
    Snapshot,
    /// Mutation preparation, including blocking-pool queue time.
    Validation,
    /// Durable request admission and exact-plan recovery.
    Admission,
    /// Entire graph execution, including decision and visibility catch-up.
    Execute,
    /// Begin, participant preparation and decision recovery (inclusive).
    Decision,
    /// One participant's preparation attempt, including history synchronization.
    Prepare,
    /// Retaining history for preparation or complete-cohort consumption.
    HistorySync,
    /// Complete-cohort catch-up and final evidence (inclusive).
    Cohort,
    /// Applying retained decisions for one cohort member.
    Consume,
    /// Checking terminal prefix evidence for one cohort member.
    Visibility,
}
/// Number of phase slots, independent of graph/history/request size.
pub const PHASE_COUNT: usize = Phase::Visibility as usize + 1;
/// Aggregated elapsed time and invocations, including interrupted invocations.
#[derive(Debug, Clone, Copy, Default)]
pub struct PhaseTiming {
    pub calls: u64,
    pub elapsed: Duration,
}
/// Local execution completion, not delivery or durable transaction outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completion {
    /// The handler returned a Write reply, possibly aborted or not yet visible.
    Returned,
    /// The handler returned an error, which may leave a durable unknown outcome.
    Error,
    /// The handler was dropped (deadline, shutdown or cancellation).
    Interrupted,
}
/// One local Submit attempt. No Plan, graph values, or error strings are retained.
#[derive(Debug, Clone)]
pub struct Report {
    pub external_id: [u8; 16],
    pub completion: Completion,
    /// From Submit dispatch through return/drop, excluding response encoding/delivery.
    pub elapsed: Duration,
    /// Indexed by `Phase as usize`; zero calls means the phase was not reached.
    pub phases: [PhaseTiming; PHASE_COUNT],
}
/// Drain without blocking the gateway. Reports are best effort and may be dropped.
pub type Reports = mpsc::Receiver<Report>;
pub(crate) type Sender = mpsc::SyncSender<Report>;
pub(crate) fn channel() -> (Sender, Reports) {
    mpsc::sync_channel(REPORT_CAPACITY)
}

#[derive(Clone, Default)]
pub(crate) struct Recorder(Option<Arc<Mutex<[PhaseTiming; PHASE_COUNT]>>>);
impl Recorder {
    pub(crate) fn span(&self, phase: Phase) -> Span<'_> {
        Span {
            recorder: self,
            phase,
            start: self.0.as_ref().map(|_| Instant::now()),
        }
    }
}
pub(crate) struct Span<'a> {
    recorder: &'a Recorder,
    phase: Phase,
    start: Option<Instant>,
}
impl Drop for Span<'_> {
    fn drop(&mut self) {
        if let (Some(state), Some(start)) = (&self.recorder.0, self.start) {
            let elapsed = start.elapsed();
            let mut state = state.lock().unwrap_or_else(|e| e.into_inner());
            let slot = &mut state[self.phase as usize];
            slot.calls = slot.calls.saturating_add(1);
            slot.elapsed = slot.elapsed.saturating_add(elapsed);
        }
    }
}
pub(crate) struct Attempt<'a> {
    sender: Option<&'a Sender>,
    start: Option<Instant>,
    external_id: [u8; 16],
    pub(crate) recorder: Recorder,
    pub(crate) completion: Completion,
}
impl<'a> Attempt<'a> {
    pub(crate) fn new(sender: Option<&'a Sender>, external_id: [u8; 16]) -> Self {
        Self {
            sender,
            start: sender.map(|_| Instant::now()),
            external_id,
            recorder: Recorder(
                sender.map(|_| Arc::new(Mutex::new([PhaseTiming::default(); PHASE_COUNT]))),
            ),
            completion: Completion::Interrupted,
        }
    }
}
impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        if let (Some(sender), Some(start), Some(state)) =
            (self.sender, self.start, &self.recorder.0)
        {
            let phases = *state.lock().unwrap_or_else(|e| e.into_inner());
            // Never await, retry, log, or propagate a diagnostic delivery error.
            let _ = sender.try_send(Report {
                external_id: self.external_id,
                completion: self.completion,
                elapsed: start.elapsed(),
                phases,
            });
        }
    }
}
