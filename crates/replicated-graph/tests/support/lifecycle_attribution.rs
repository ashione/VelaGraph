//! Bounded, best-effort diagnostics for this fixture attempt, not recovery authority.
use replicated_graph::managed_submission_timing::{
    Completion, Phase, PhaseTiming, Reports, PHASE_COUNT,
};
use serde::Serialize;
use std::{fs::File, io::Read, path::Path, time::Instant};

const PHASES: [Phase; PHASE_COUNT] = [
    Phase::Queue,
    Phase::Canonical,
    Phase::Recovery,
    Phase::Snapshot,
    Phase::Validation,
    Phase::Admission,
    Phase::Execute,
    Phase::Decision,
    Phase::Prepare,
    Phase::HistorySync,
    Phase::Cohort,
    Phase::Consume,
    Phase::Visibility,
];
const PROC_LIMIT: u64 = 16 * 1024;

pub(super) struct Attribution {
    reports: Option<Reports>,
    source: String,
    elf: String,
    started: Instant,
    phases: [PhaseTiming; PHASE_COUNT],
    completions: [u64; 3],
    elapsed_ns: u128,
    sequence: u64,
    ordinal: u64,
    complete: bool,
}

impl Attribution {
    pub(super) fn new(reports: Option<Reports>, source: &str, elf: &str) -> Self {
        Self {
            reports,
            source: source.into(),
            elf: elf.into(),
            started: Instant::now(),
            phases: [PhaseTiming::default(); PHASE_COUNT],
            completions: [0; 3],
            elapsed_ns: 0,
            sequence: 0,
            ordinal: 0,
            complete: false,
        }
    }

    pub(super) fn drain(&mut self) {
        let Some(reports) = &self.reports else { return };
        // A serial fixture does not submit while draining. Still cap this loop
        // at the production queue capacity, independent of producer scheduling.
        for report in reports
            .try_iter()
            .take(replicated_graph::managed_submission_timing::REPORT_CAPACITY)
        {
            let slot = match report.completion {
                Completion::Returned => 0,
                Completion::Error => 1,
                Completion::Interrupted => 2,
            };
            self.completions[slot] = self.completions[slot].saturating_add(1);
            self.elapsed_ns = self.elapsed_ns.saturating_add(report.elapsed.as_nanos());
            for (total, phase) in self.phases.iter_mut().zip(report.phases) {
                total.calls = total.calls.saturating_add(phase.calls);
                total.elapsed = total.elapsed.saturating_add(phase.elapsed);
            }
        }
    }

    pub(super) fn emit(
        &mut self,
        stage: &str,
        ordinal: u64,
        complete: bool,
        processes: &[(&str, u32)],
    ) {
        if self.reports.is_none() {
            return;
        }
        self.drain();
        self.ordinal = ordinal;
        self.complete = complete;
        self.sequence = self.sequence.saturating_add(1);
        // Callers pass only the gateway owner and its three currently owned
        // children, before reaping them. Never enumerate unrelated processes.
        let processes: Vec<_> = processes.iter().take(4).map(|(role, pid)| {
            serde_json::json!({"role": role, "pid": pid, "linux_proc": process_sample(*pid)})
        }).collect();
        let phases: Vec<_> = PHASES
            .iter()
            .map(|phase| {
                let total = self.phases[*phase as usize];
                serde_json::json!({"phase": format!("{phase:?}"), "calls": total.calls,
                "elapsed_ns": total.elapsed.as_nanos()})
            })
            .collect();
        eprintln!(
            "byte-bound lifecycle attribution: {}",
            serde_json::json!({
                "format": 1, "source_sha256": self.source, "elf_sha256": self.elf,
                "owner_pid": std::process::id(), "sequence": self.sequence,
                "attempt_elapsed_ns": self.started.elapsed().as_nanos(),
                "verified_ordinal": ordinal, "lifecycle_complete": complete, "stage": stage,
                "scope": "this attempt; best effort; missing reports are unknown; nested phases must not be summed",
                "reports": {"returned": self.completions[0], "error": self.completions[1],
                    "interrupted": self.completions[2], "elapsed_ns": self.elapsed_ns, "phases": phases},
                "processes": processes,
            })
        );
    }

    #[cfg(test)]
    pub(super) fn received(&mut self) -> u64 {
        self.drain();
        self.completions.iter().sum()
    }
}

impl Drop for Attribution {
    fn drop(&mut self) {
        // On failure/cancellation the process handles may already have changed.
        // Do not read a stale PID. Emit phase evidence only, with no success claim.
        if !self.complete {
            self.emit("incomplete_drop", self.ordinal, false, &[]);
        }
    }
}

#[derive(Debug, Serialize, PartialEq)]
struct CpuMemory {
    start_ticks: u64,
    user_ticks: u64,
    system_ticks: u64,
    resident_pages: u64,
}

#[derive(Debug, Serialize, PartialEq)]
struct Io {
    read_chars: u64,
    write_chars: u64,
    read_bytes: u64,
    write_bytes: u64,
}

#[derive(Serialize)]
struct ProcessSample {
    cpu_memory: Option<CpuMemory>,
    io: Option<Io>,
}

fn bounded_text(path: &Path) -> Option<String> {
    let mut bytes = Vec::new();
    File::open(path)
        .ok()?
        .take(PROC_LIMIT + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() as u64 > PROC_LIMIT {
        return None;
    }
    String::from_utf8(bytes).ok()
}

fn process_sample(pid: u32) -> ProcessSample {
    let root = Path::new("/proc").join(pid.to_string());
    ProcessSample {
        cpu_memory: bounded_text(&root.join("stat")).and_then(|text| parse_stat(&text, pid)),
        io: bounded_text(&root.join("io")).and_then(|text| parse_io(&text)),
    }
}

fn parse_stat(text: &str, pid: u32) -> Option<CpuMemory> {
    // comm may include spaces and ')'; fields after its final ')' begin at 3.
    let (prefix, rest) = text.rsplit_once(") ")?;
    if prefix.split_once(" (")?.0.parse::<u32>().ok()? != pid {
        return None;
    }
    let fields: Vec<_> = rest.split_whitespace().take(23).collect();
    Some(CpuMemory {
        user_ticks: fields.get(11)?.parse().ok()?,
        system_ticks: fields.get(12)?.parse().ok()?,
        start_ticks: fields.get(19)?.parse().ok()?,
        resident_pages: fields.get(21)?.parse().ok()?,
    })
}

fn parse_io(text: &str) -> Option<Io> {
    fn field(text: &str, key: &str) -> Option<u64> {
        let mut values = text
            .lines()
            .filter_map(|line| line.split_once(':'))
            .filter(|(name, _)| *name == key)
            .map(|(_, value)| value.trim().parse::<u64>().ok());
        let value = values.next()??;
        if values.next().is_some() {
            return None;
        }
        Some(value)
    }
    Some(Io {
        read_chars: field(text, "rchar")?,
        write_chars: field(text, "wchar")?,
        read_bytes: field(text, "read_bytes")?,
        write_bytes: field(text, "write_bytes")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attribution_aggregates_bounded_reports_and_retains_completion_types() {
        use replicated_graph::managed_submission_timing::{Report, REPORT_CAPACITY};
        let (sender, receiver) = std::sync::mpsc::sync_channel(REPORT_CAPACITY);
        let mut attribution = Attribution::new(Some(receiver), "source", "elf");
        for completion in [
            Completion::Returned,
            Completion::Error,
            Completion::Interrupted,
        ] {
            let mut phases = [PhaseTiming::default(); PHASE_COUNT];
            phases[Phase::Recovery as usize] = PhaseTiming {
                calls: 2,
                elapsed: std::time::Duration::from_nanos(5),
            };
            sender
                .send(Report {
                    external_id: [42; 16],
                    completion,
                    elapsed: std::time::Duration::from_nanos(9),
                    phases,
                })
                .unwrap();
        }
        drop(sender);
        assert_eq!(attribution.received(), 3);
        assert_eq!(attribution.received(), 3); // draining cannot count twice
        assert_eq!(attribution.completions, [1, 1, 1]);
        assert_eq!(attribution.elapsed_ns, 27);
        assert_eq!(attribution.phases[Phase::Recovery as usize].calls, 6);
        assert_eq!(
            attribution.phases[Phase::Recovery as usize]
                .elapsed
                .as_nanos(),
            15
        );
        assert_eq!(attribution.phases[Phase::Admission as usize].calls, 0);
        attribution.complete = true;
        let mut disabled = Attribution::new(None, "source", "elf");
        assert_eq!(disabled.received(), 0);
        disabled.emit("disabled", 99, true, &[]);
        assert_eq!(disabled.sequence, 0);
    }

    #[test]
    fn attribution_parses_counters_without_pid_or_command_confusion() {
        let fields = (3..=25)
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        let stat = format!("41 (a ) tricky (name)) {fields}");
        assert_eq!(
            parse_stat(&stat, 41),
            Some(CpuMemory {
                user_ticks: 14,
                system_ticks: 15,
                start_ticks: 22,
                resident_pages: 24,
            })
        );
        assert!(parse_stat(&stat, 42).is_none());
        assert!(parse_stat("41 (a) R 1", 41).is_none());
        assert!(parse_stat(&stat.replace(" 24 ", " -1 "), 41).is_none());
        let io = "rchar: 1\nwchar: 2\nread_bytes: 3\nwrite_bytes: 4\nsyscr: 5\n";
        assert_eq!(
            parse_io(io),
            Some(Io {
                read_chars: 1,
                write_chars: 2,
                read_bytes: 3,
                write_bytes: 4
            })
        );
        for invalid in [
            io.replace("rchar: 1", "rchar: -1"),
            io.replace("write_bytes: 4", "other: 4"),
            format!("{io}rchar: 9\n"),
        ] {
            assert!(parse_io(&invalid).is_none());
        }
    }

    #[test]
    fn attribution_missing_or_oversized_counters_are_unknown() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("proc");
        assert!(bounded_text(&path).is_none());
        std::fs::write(&path, vec![b'a'; PROC_LIMIT as usize]).unwrap();
        assert_eq!(bounded_text(&path).unwrap().len(), PROC_LIMIT as usize);
        std::fs::write(&path, vec![b'a'; PROC_LIMIT as usize + 1]).unwrap();
        assert!(bounded_text(&path).is_none());
        std::fs::write(&path, [0xff]).unwrap();
        assert!(bounded_text(&path).is_none());
        assert!(parse_io("").is_none());
    }
}
