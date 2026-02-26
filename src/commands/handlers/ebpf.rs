//! eBPF tracing command handlers
//!
//! Commands:
//! - EBPF.STATUS - Check eBPF availability and status
//! - EBPF.PROBES - List available/attached probe points
//! - EBPF.STATS - Get aggregated probe statistics
//! - EBPF.ATTACH <probe_type> <target> - Attach a probe
//! - EBPF.DETACH <probe_id> - Detach a probe

use bytes::Bytes;
use ferrite_core::observability::ebpf::{
    EbpfConfig, EbpfManager, ProbeSpec, ProbeTarget, ProbeType,
};

#[cfg(target_os = "linux")]
use ferrite_core::observability::ebpf_linux::LinuxEbpfTracer;

use crate::protocol::Frame;

use super::err_frame;

/// Handle EBPF subcommands
pub fn handle_ebpf(subcommand: &str, args: &[String]) -> Frame {
    match subcommand {
        "STATUS" => ebpf_status(),
        "PROBES" => ebpf_probes(),
        "STATS" => ebpf_stats(),
        "ATTACH" => ebpf_attach(args),
        "DETACH" => ebpf_detach(args),
        _ => err_frame(&format!("unknown EBPF subcommand '{}'", subcommand)),
    }
}

fn ebpf_status() -> Frame {
    let manager = EbpfManager::new(EbpfConfig::default());
    let available = manager.is_available();
    let stats = manager.get_stats();

    let mut fields = vec![
        Frame::bulk("supported"),
        Frame::bulk(if available { "true" } else { "false" }),
        Frame::bulk("os"),
        Frame::bulk(std::env::consts::OS),
        Frame::bulk("probes_attached"),
        Frame::integer(stats.probes_attached as i64),
        Frame::bulk("status"),
        Frame::bulk(if available { "active" } else { "unavailable" }),
        Frame::bulk("note"),
        Frame::bulk("eBPF tracing requires Linux 5.15+ with CAP_BPF"),
    ];

    // On Linux, append native tracer diagnostics
    #[cfg(target_os = "linux")]
    {
        let linux_tracer = LinuxEbpfTracer::new();
        let linux_stats = linux_tracer.stats();
        fields.push(Frame::bulk("linux_tracefs_available"));
        fields.push(Frame::bulk(if linux_stats.tracefs_available {
            "true"
        } else {
            "false"
        }));
        fields.push(Frame::bulk("linux_kernel_version"));
        fields.push(Frame::bulk(Bytes::from(linux_stats.kernel_version)));
        fields.push(Frame::bulk("linux_kernel_meets_req"));
        fields.push(Frame::bulk(if linux_stats.meets_kernel_req {
            "true"
        } else {
            "false"
        }));
    }

    Frame::array(fields)
}

fn ebpf_probes() -> Frame {
    let manager = EbpfManager::new(EbpfConfig::default());
    let probes = manager.list_probes();

    if probes.is_empty() {
        return Frame::array(vec![
            Frame::bulk("attached_probes"),
            Frame::integer(0),
            Frame::bulk("available_types"),
            Frame::array(vec![
                Frame::bulk("kprobe"),
                Frame::bulk("uprobe"),
                Frame::bulk("tracepoint"),
                Frame::bulk("usdt"),
                Frame::bulk("perf_event"),
                Frame::bulk("raw_tracepoint"),
            ]),
        ]);
    }

    let probe_frames: Vec<Frame> = probes
        .into_iter()
        .map(|p| {
            Frame::array(vec![
                Frame::bulk("id"),
                Frame::bulk(Bytes::from(p.id.0)),
                Frame::bulk("name"),
                Frame::bulk(Bytes::from(p.name)),
                Frame::bulk("type"),
                Frame::bulk(Bytes::from(p.probe_type.to_string())),
                Frame::bulk("target"),
                Frame::bulk(Bytes::from(p.target.to_string())),
                Frame::bulk("status"),
                Frame::bulk(Bytes::from(p.status.to_string())),
                Frame::bulk("events_captured"),
                Frame::integer(p.events_captured as i64),
            ])
        })
        .collect();

    Frame::array(probe_frames)
}

fn ebpf_stats() -> Frame {
    let manager = EbpfManager::new(EbpfConfig::default());
    let stats = manager.get_stats();

    let mut fields = vec![
        Frame::bulk("probes_attached"),
        Frame::integer(stats.probes_attached as i64),
        Frame::bulk("events_captured"),
        Frame::integer(stats.events_captured as i64),
        Frame::bulk("bytes_processed"),
        Frame::integer(stats.bytes_processed as i64),
        Frame::bulk("errors"),
        Frame::integer(stats.errors as i64),
        Frame::bulk("uptime_secs"),
        Frame::integer(stats.uptime_secs as i64),
    ];

    // On Linux, append native tracer statistics
    #[cfg(target_os = "linux")]
    {
        let linux_tracer = LinuxEbpfTracer::new();
        let linux_stats = linux_tracer.stats();
        fields.push(Frame::bulk("linux_active_probes"));
        fields.push(Frame::integer(linux_stats.active_probes as i64));
        fields.push(Frame::bulk("linux_total_events"));
        fields.push(Frame::integer(linux_stats.total_events as i64));
        fields.push(Frame::bulk("linux_uptime_secs"));
        fields.push(Frame::integer(linux_stats.uptime.as_secs() as i64));
    }

    Frame::array(fields)
}

fn ebpf_attach(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame(
            "wrong number of arguments for 'EBPF ATTACH': requires <probe_type> <target>",
        );
    }

    let probe_type = match args[0].to_lowercase().as_str() {
        "kprobe" => ProbeType::Kprobe,
        "uprobe" => ProbeType::Uprobe,
        "tracepoint" => ProbeType::Tracepoint,
        "usdt" => ProbeType::Usdt,
        "perf_event" => ProbeType::PerfEvent,
        "raw_tracepoint" => ProbeType::RawTracepoint,
        other => return err_frame(&format!("unknown probe type: {}", other)),
    };

    let target = match probe_type {
        ProbeType::Kprobe => ProbeTarget::Kernel {
            function: args[1].clone(),
        },
        ProbeType::Tracepoint | ProbeType::RawTracepoint => {
            let parts: Vec<&str> = args[1].splitn(2, ':').collect();
            if parts.len() != 2 {
                return err_frame("tracepoint target must be 'category:name'");
            }
            ProbeTarget::Tracepoint {
                category: parts[0].to_string(),
                name: parts[1].to_string(),
            }
        }
        _ => ProbeTarget::Userspace {
            binary: args[1].clone(),
            function: args.get(2).cloned().unwrap_or_default(),
        },
    };

    let spec = ProbeSpec {
        name: args[1].clone(),
        probe_type,
        target,
        filter: args.get(3).cloned(),
    };

    let manager = EbpfManager::new(EbpfConfig::default());
    match manager.attach_probe(spec) {
        Ok(id) => Frame::array(vec![
            Frame::bulk("probe_id"),
            Frame::bulk(Bytes::from(id.0)),
            Frame::bulk("status"),
            Frame::bulk("attached"),
        ]),
        Err(e) => Frame::array(vec![
            Frame::bulk("status"),
            Frame::bulk("failed"),
            Frame::bulk("error"),
            Frame::bulk(Bytes::from(e.to_string())),
        ]),
    }
}

fn ebpf_detach(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'EBPF DETACH': requires <probe_id>");
    }

    let probe_id = ferrite_core::observability::ebpf::ProbeId(args[0].clone());
    let manager = EbpfManager::new(EbpfConfig::default());

    match manager.detach_probe(&probe_id) {
        Ok(()) => Frame::array(vec![
            Frame::bulk("probe_id"),
            Frame::bulk(Bytes::from(args[0].clone())),
            Frame::bulk("status"),
            Frame::bulk("detached"),
        ]),
        Err(e) => Frame::array(vec![
            Frame::bulk("probe_id"),
            Frame::bulk(Bytes::from(args[0].clone())),
            Frame::bulk("status"),
            Frame::bulk("failed"),
            Frame::bulk("error"),
            Frame::bulk(Bytes::from(e.to_string())),
        ]),
    }
}
