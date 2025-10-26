#!/usr/bin/env python3
import sys
import json
from typing import Any, Dict, Optional

def classify_performance(
    timing: Dict[str, Any],
    cpu: Dict[str, Any],
    cache: Dict[str, Any],
    memory: Dict[str, Any],
    concurrency: Dict[str, Any],
    syscalls: Dict[str, Any],
) -> Dict[str, Any]:
    """Compute performance classification from profiler JSON sections.
    Returns a dict with keys: primary_bottleneck, parallel_potential, confidence, reasons.
    """
    def get(d: Optional[Dict[str, Any]], k: str):
        return (d or {}).get(k)

    elapsed = get(timing, "elapsed_s") or 0.0
    wait_time = get(timing, "wait_time_s") or 0.0
    wait_frac = (wait_time / elapsed) if elapsed else 0.0

    ipc = get(cpu, "ipc")
    branch_miss_rate = get(cpu, "branch_miss_rate_pct")
    frontend_stall_pct = get(cpu, "frontend_stall_pct")
    backend_stall_pct = get(cpu, "backend_stall_pct")

    l1d_miss_rate = get(cache, "l1d_miss_rate_pct")
    llc_miss_rate = get(cache, "llc_miss_rate_pct")

    dtlb_miss_rate = get(memory, "dtlb_miss_rate_pct")
    ram_total_bw = get(memory, "ram_total_bandwidth_mbps")

    syscalls_total_seconds = None
    try:
        total = (syscalls or {}).get("total")
        if isinstance(total, dict):
            syscalls_total_seconds = total.get("seconds")
    except Exception:
        syscalls_total_seconds = None

    # Detect short-lived / startup-dominated workload
    # Criteria: very short elapsed time AND syscalls dominated by execve/mmap (startup)
    short_app = False
    if elapsed and elapsed < 0.1:  # less than 100ms
        # Check if top syscalls are startup-related
        try:
            rows = (syscalls or {}).get("syscalls", [])
            if rows:
                startup_syscalls = {"execve", "mmap", "munmap", "mprotect", "brk", "access", "openat", "close", "fstat", "read"}
                startup_time = sum(r.get("seconds", 0.0) for r in rows if r.get("syscall") in startup_syscalls)
                if syscalls_total_seconds and startup_time / syscalls_total_seconds > 0.7:
                    short_app = True
        except Exception:
            pass

    # Heuristics
    io_cond = (wait_frac >= 0.2) or (elapsed and syscalls_total_seconds and (syscalls_total_seconds / elapsed) >= 0.3)
    # Memory-bound heuristic
    # Primary signals: high miss rates (LLC, L1D, TLB). Backend stalls strengthen the case when available.
    mem_miss_signals = 0
    if llc_miss_rate is not None and llc_miss_rate >= 5.0:
        mem_miss_signals += 1
    if l1d_miss_rate is not None and l1d_miss_rate >= 10.0:
        mem_miss_signals += 1
    if dtlb_miss_rate is not None and dtlb_miss_rate >= 1.0:
        mem_miss_signals += 1

    # Consider memory-bound if:
    # - We have stall data and it shows backend stalls with at least one miss signal, OR
    # - Stall data is missing but miss rates are very strong, OR
    # - Total RAM bandwidth is very high (streaming), combined with at least one miss signal.
    mem_cond = False
    if backend_stall_pct is not None:
        mem_cond = (mem_miss_signals >= 1) and (backend_stall_pct >= 20.0)
    else:
        strong_miss = (llc_miss_rate is not None and llc_miss_rate >= 30.0) or (mem_miss_signals >= 2)
        high_bw = (ram_total_bw is not None and ram_total_bw >= 1000.0)  # ~1 GB/s threshold
        mem_cond = strong_miss or (high_bw and mem_miss_signals >= 1)

    branch_cond = (branch_miss_rate is not None and branch_miss_rate >= 5.0)
    frontend_cond = (frontend_stall_pct is not None and frontend_stall_pct >= 30.0)
    backend_cond = (backend_stall_pct is not None and backend_stall_pct >= 30.0)

    # CPU compute-bound: decent IPC and low stalls/misses
    cpu_compute_cond = (
        (ipc is not None and ipc >= 1.5)
        and (frontend_stall_pct is None or frontend_stall_pct < 20.0)
        and (backend_stall_pct is None or backend_stall_pct < 20.0)
        and (l1d_miss_rate is None or l1d_miss_rate < 10.0)
        and (llc_miss_rate is None or llc_miss_rate < 5.0)
    )

    # Primary bottleneck selection
    primary_bottleneck = "CPU"
    reasons = []
    
    # Override: short-lived app gets "No bottleneck" classification
    if short_app:
        primary_bottleneck = "No bottleneck"
        reasons.append({
            "elapsed_s": elapsed,
            "note": "Short-lived app (< 100ms). Execution dominated by startup syscalls (execve/mmap). No meaningful steady-state bottleneck detected."
        })
    elif io_cond:
        primary_bottleneck = "I/O/Wait"
        reasons.append({"io_wait_frac": round(wait_frac, 3), "syscalls_total_seconds": syscalls_total_seconds})
    elif mem_cond:
        primary_bottleneck = "Memory"
        reasons.append({
            "llc_miss_rate_pct": llc_miss_rate,
            "l1d_miss_rate_pct": l1d_miss_rate,
            "dtlb_miss_rate_pct": dtlb_miss_rate,
            "backend_stall_pct": backend_stall_pct,
            "ram_total_bandwidth_mbps": ram_total_bw
        })
    elif branch_cond:
        primary_bottleneck = "CPU"
        reasons.append({"branch_miss_rate_pct": branch_miss_rate})
    elif frontend_cond or backend_cond:
        primary_bottleneck = "CPU"
        reasons.append({
            "frontend_stall_pct": frontend_stall_pct,
            "backend_stall_pct": backend_stall_pct
        })
    elif cpu_compute_cond:
        primary_bottleneck = "CPU"
        reasons.append({"ipc": ipc})
    else:
        # Default to CPU when nothing else dominates; add a small context reason
        primary_bottleneck = "CPU"
        reasons.append({
            "ipc": ipc,
            "frontend_stall_pct": frontend_stall_pct,
            "backend_stall_pct": backend_stall_pct,
            "branch_miss_rate_pct": branch_miss_rate
        })

    # Parallel potential heuristic
    threads = get(concurrency, "threads") or 1
    cpu_util_per_core = get(timing, "cpu_utilization_per_core_pct") or 0.0
    if primary_bottleneck == "No bottleneck":
        parallel_potential = "n/a"
    elif primary_bottleneck == "Memory" or io_cond:
        parallel_potential = "low"
    elif primary_bottleneck == "CPU" and threads <= 1 and (ipc or 0) >= 1.0:
        parallel_potential = "high"
    elif primary_bottleneck == "CPU" and (ipc or 0) >= 0.9 and cpu_util_per_core < 85.0:
        parallel_potential = "medium"
    else:
        parallel_potential = "low"

    # Confidence score (rough): number of signals supporting the chosen bottleneck
    confidence = 0.5
    if primary_bottleneck == "No bottleneck":
        confidence = 0.9  # High confidence for short apps
    elif primary_bottleneck == "Memory":
        score = 0
        if llc_miss_rate is not None and llc_miss_rate >= 5.0: score += 1
        if l1d_miss_rate is not None and l1d_miss_rate >= 10.0: score += 1
        if dtlb_miss_rate is not None and dtlb_miss_rate >= 1.0: score += 1
        if backend_stall_pct is not None and backend_stall_pct >= 20.0: score += 1
        if ram_total_bw is not None and ram_total_bw >= 1000.0: score += 1
        confidence = 0.35 + 0.13 * min(score, 4)
    elif primary_bottleneck == "I/O/Wait":
        score = 0
        if wait_frac >= 0.2: score += 1
        if elapsed and syscalls_total_seconds and (syscalls_total_seconds / elapsed) >= 0.3: score += 1
        confidence = 0.4 + 0.3 * score
    else:  # CPU
        score = 0
        if ipc is not None and ipc >= 1.5: score += 1
        if frontend_stall_pct is not None and frontend_stall_pct < 20.0: score += 1
        if backend_stall_pct is not None and backend_stall_pct < 20.0: score += 1
        if l1d_miss_rate is not None and l1d_miss_rate < 10.0: score += 1
        if llc_miss_rate is not None and llc_miss_rate < 5.0: score += 1
        confidence = 0.3 + 0.15 * score
    # Enrich reasons to always be informative
    try:
        if primary_bottleneck == "I/O/Wait":
            # Include top-3 syscalls by time if available
            rows = (syscalls or {}).get("syscalls")
            if isinstance(rows, list) and rows:
                top = sorted(rows, key=lambda r: r.get("seconds", 0.0), reverse=True)[:3]
                reasons.append({
                    "top_syscalls_by_time": [
                        {
                            "syscall": r.get("syscall"),
                            "seconds": r.get("seconds"),
                            "pct_time": r.get("pct_time")
                        } for r in top
                    ]
                })
        # No extra summary for CPU/Memory to avoid redundancy with reasons above
    except Exception:
        pass

    # Generate actionable optimization suggestions based on bottleneck and metrics
    suggestions = []
    
    if primary_bottleneck == "No bottleneck":
        suggestions.append("Short-lived application (< 100ms runtime). Startup overhead dominates execution. No optimization opportunities for steady-state performance. If running many instances, consider batching or keeping process alive.")
    
    # Detect potential profiling artifacts: high IPC + low syscall overhead but code likely does I/O
    # This happens when stdout is redirected during profiling, causing buffering
    try:
        rows = (syscalls or {}).get("syscalls", [])
        write_calls = sum(r.get("calls", 0) for r in rows if r.get("syscall") == "write")
        total_syscall_time = syscalls_total_seconds or 0
        
        if primary_bottleneck == "CPU" and write_calls > 0 and elapsed > 0:
            # If writes exist but contributed very little time, output was likely buffered/redirected
            if total_syscall_time / elapsed < 0.1 and write_calls > 100:
                suggestions.append(f"Note: Profile shows CPU-bound with {write_calls} write calls but minimal I/O time. If this program writes to stdout, profiling with redirected output may have caused buffering that masks I/O bottlenecks. Re-run without redirection for accurate interactive performance.")
    except Exception:
        pass
    
    if primary_bottleneck == "I/O/Wait":
        # Check if dominated by write syscalls
        write_heavy = False
        try:
            rows = (syscalls or {}).get("syscalls", [])
            for r in rows:
                if r.get("syscall") == "write" and r.get("pct_time", 0) > 90:
                    write_heavy = True
                    calls = r.get("calls", 0)
                    if calls > 10000:
                        suggestions.append("High write syscall count detected. Consider buffering output or using batch writes to reduce syscall overhead.")
                    break
        except Exception:
            pass
        
        if not write_heavy:
            suggestions.append("I/O-bound workload detected. Consider async I/O, memory-mapped files, or reducing syscall frequency.")
        
        # Check for frequent context switches
        ctx_switches = get(concurrency, "context_switches") or 0
        if elapsed and ctx_switches / elapsed > 100:
            suggestions.append("High context switch rate. Consider reducing lock contention or thread count.")
    
    elif primary_bottleneck == "Memory":
        # High LLC miss rate
        if llc_miss_rate is not None and llc_miss_rate >= 30.0:
            suggestions.append(f"Very high LLC miss rate ({llc_miss_rate:.1f}%). Working set likely exceeds cache size. Consider reducing memory footprint, using cache-aware data structures, or blocking algorithms.")
        elif llc_miss_rate is not None and llc_miss_rate >= 10.0:
            suggestions.append(f"High LLC miss rate ({llc_miss_rate:.1f}%). Consider improving data locality (e.g., structure-of-arrays vs array-of-structures, prefetching).")
        
        # High L1D miss rate
        if l1d_miss_rate is not None and l1d_miss_rate >= 15.0:
            suggestions.append(f"High L1 data cache miss rate ({l1d_miss_rate:.1f}%). Consider improving spatial/temporal locality or reducing random access patterns.")
        
        # High TLB miss rate
        if dtlb_miss_rate is not None and dtlb_miss_rate >= 2.0:
            suggestions.append(f"High TLB miss rate ({dtlb_miss_rate:.1f}%). Large memory footprint with scattered access. Consider huge pages or reducing working set.")
        
        # High RAM bandwidth
        if ram_total_bw is not None and ram_total_bw >= 5000.0:
            suggestions.append(f"High RAM bandwidth usage ({ram_total_bw:.0f} MB/s). Memory-bandwidth bound. Consider compression, reducing data movement, or compute-to-memory-access ratio improvements.")
    
    elif primary_bottleneck == "CPU":
        # Low IPC
        if ipc is not None and ipc < 1.0:
            suggestions.append(f"Low IPC ({ipc:.2f}). Pipeline inefficiency detected. Check for branch mispredictions, data dependencies, or inadequate instruction-level parallelism.")
        
        # High frontend stalls
        if frontend_stall_pct is not None and frontend_stall_pct >= 30.0:
            suggestions.append(f"High frontend stalls ({frontend_stall_pct:.1f}%). Instruction fetch bottleneck. Consider code layout optimization, reducing instruction cache misses, or removing excessive branches.")
        
        # High backend stalls
        if backend_stall_pct is not None and backend_stall_pct >= 30.0:
            suggestions.append(f"High backend stalls ({backend_stall_pct:.1f}%). Execution bottleneck. Check for long-latency operations, resource conflicts, or memory access patterns.")
        
        # High branch miss rate
        if branch_miss_rate is not None and branch_miss_rate >= 5.0:
            suggestions.append(f"High branch miss rate ({branch_miss_rate:.1f}%). Consider profile-guided optimization, reducing unpredictable branches, or using branchless code patterns.")
        
        # Good IPC and single-threaded
        threads = get(concurrency, "threads") or 1
        if ipc is not None and ipc >= 1.5 and threads == 1:
            suggestions.append(f"Good IPC ({ipc:.2f}) with single-threaded execution. Consider parallelization to utilize multiple cores.")
        
        # Vectorization opportunity
        if ipc is not None and ipc >= 1.0 and ipc < 2.0:
            suggestions.append("Moderate IPC. Consider SIMD vectorization (AVX2/AVX-512) if working with arrays or loops with independent iterations.")

    return {
        "primary_bottleneck": primary_bottleneck,
        "parallel_potential": parallel_potential,
        "confidence": round(min(max(confidence, 0.0), 1.0), 2),
        "reasons": reasons,
        "suggestions": suggestions
    }


def classify_result(result: Dict[str, Any]) -> Dict[str, Any]:
    return classify_performance(
        result.get("timing", {}),
        result.get("cpu", {}),
        result.get("cache", {}),
        result.get("memory", {}),
        result.get("concurrency", {}),
        result.get("syscalls", {}),
    )


def main():
    import argparse
    p = argparse.ArgumentParser(description="Analyze profiler JSON and emit performance classification.")
    p.add_argument("json", nargs="?", help="Path to profiler JSON file or '-' for stdin. If omitted, reads stdin.")
    p.add_argument("--augment", action="store_true", help="Print original JSON with added 'performance_classification'.")
    args = p.parse_args()

    # Read JSON
    if not args.json or args.json == "-":
        data = json.load(sys.stdin)
    else:
        with open(args.json, "r") as f:
            data = json.load(f)

    cls = classify_result(data)
    if args.augment:
        data["performance_classification"] = cls
        json.dump(data, sys.stdout, indent=2)
    else:
        json.dump(cls, sys.stdout, indent=2)
    print()

if __name__ == "__main__":
    main()
