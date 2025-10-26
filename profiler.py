#!/usr/bin/env python3

import argparse
import subprocess
import json
import tempfile
import os
import sys
import shutil
import time
from typing import Dict, Any


def ensure_keys(dest: dict, defaults: dict) -> None:
    """Ensure that each key from defaults exists in dest; set to default value when missing.

    This makes the output schema stable: absent measurements become explicit None values.
    """
    for k, v in defaults.items():
        if k not in dest:
            dest[k] = v

def run_perf_stat(binary: str, args: list) -> Dict[str, Any]:
    # Extended event list for comprehensive performance analysis
    events = [
        # Core timing and execution
        "task-clock,instructions,cycles,ref-cycles",
        # Branching
        "branches,branch-misses",
        # L1 Data Cache
        "L1-dcache-loads,L1-dcache-load-misses,L1-dcache-stores",
        # L1 Instruction Cache
        "L1-icache-loads,L1-icache-load-misses",
        # Last Level Cache (LLC/L3)
        "LLC-loads,LLC-load-misses,LLC-stores,LLC-store-misses",
        # TLB (Translation Lookaside Buffer)
        "dTLB-loads,dTLB-load-misses,iTLB-loads,iTLB-load-misses",
        # Pipeline stalls
        "stalled-cycles-frontend,stalled-cycles-backend",
        # System events
        "context-switches,cpu-migrations,page-faults,minor-faults,major-faults",
        # Alignment and other faults
        "alignment-faults,emulation-faults",
    ]
    
    cmd = ["perf", "stat", "-e", ",".join(events), binary] + args
    with tempfile.NamedTemporaryFile(delete=False, mode="w+") as tmp:
        try:
            result = subprocess.run(cmd, stdout=tmp, stderr=subprocess.STDOUT, text=True)
            tmp.flush()
            tmp.seek(0)
            output = tmp.read()
        finally:
            os.unlink(tmp.name)
    
    metrics = {}
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        
        try:
            value_str = parts[0].replace(",", "").replace("<not", "").replace("supported>", "").strip()
            if not value_str or value_str == "":
                continue
            value = float(value_str) if "." in value_str else int(value_str)
        except (ValueError, IndexError):
            continue
        
        line_lower = line.lower()
        
        # Timing
        if "task-clock" in line_lower:
            # perf prints like: <val> msec task-clock
            try:
                metrics["task_clock_ms"] = float(parts[0].replace(",", ""))
            except Exception:
                pass
        elif "seconds time elapsed" in line_lower:
            metrics["elapsed_s"] = float(parts[0])
        elif "seconds user" in line_lower:
            metrics["user_s"] = float(parts[0])
        elif "seconds sys" in line_lower:
            metrics["sys_s"] = float(parts[0])
        
        # Core execution
        elif "instructions" in line_lower:
            metrics["instructions"] = value
        elif "ref-cycles" in line_lower or "reference cycles" in line_lower:
            metrics["ref_cycles"] = value
        elif "cycles" in line_lower and "ref-cycles" not in line_lower:
            metrics["cycles"] = value
        
        # Branching
        elif "branch-misses" in line_lower:
            metrics["branch_misses"] = value
        elif "branches" in line_lower:
            metrics["branches"] = value
        
        # L1 Data Cache
        elif "l1-dcache-loads" in line_lower or "l1d.replacement" in line_lower:
            metrics["l1_dcache_loads"] = value
        elif "l1-dcache-load-misses" in line_lower:
            metrics["l1_dcache_load_misses"] = value
        elif "l1-dcache-stores" in line_lower:
            metrics["l1_dcache_stores"] = value
        
        # L1 Instruction Cache
        elif "l1-icache-loads" in line_lower or "l1i.replacements" in line_lower or "l1i.replacement" in line_lower:
            metrics["l1_icache_loads"] = value
        elif "l1-icache-load-misses" in line_lower:
            metrics["l1_icache_load_misses"] = value
        
        # LLC (L3)
        elif "llc-loads" in line_lower:
            metrics["llc_loads"] = value
        elif "llc-load-misses" in line_lower:
            metrics["llc_load_misses"] = value
        elif "llc-stores" in line_lower:
            metrics["llc_stores"] = value
        elif "llc-store-misses" in line_lower:
            metrics["llc_store_misses"] = value
        
        # TLB
        elif "dtlb-loads" in line_lower or "dtlb-loads" in line:
            metrics["dtlb_loads"] = value
        elif "dtlb-load-misses" in line_lower:
            metrics["dtlb_load_misses"] = value
        elif "itlb-loads" in line_lower:
            metrics["itlb_loads"] = value
        elif "itlb-load-misses" in line_lower:
            metrics["itlb_load_misses"] = value
        
        # Stalls
        elif "stalled-cycles-frontend" in line_lower:
            metrics["stalled_cycles_frontend"] = value
        elif "stalled-cycles-backend" in line_lower:
            metrics["stalled_cycles_backend"] = value
        
        # System
        elif "context-switches" in line_lower:
            metrics["context_switches"] = value
        elif "cpu-migrations" in line_lower:
            metrics["cpu_migrations"] = value
        elif "page-faults" in line_lower and "major" not in line_lower and "minor" not in line_lower:
            metrics["page_faults"] = value
        elif "minor-faults" in line_lower:
            metrics["minor_faults"] = value
        elif "major-faults" in line_lower:
            metrics["major_faults"] = value
        
        # Faults
        elif "alignment-faults" in line_lower:
            metrics["alignment_faults"] = value
        elif "emulation-faults" in line_lower:
            metrics["emulation_faults"] = value
    
    return metrics

def run_time_v_timing(binary: str, args: list) -> Dict[str, Any]:
    """Run /usr/bin/time -v and parse elapsed, user, sys seconds.
    Returns keys: elapsed_s, user_s, sys_s, wait_time_s (when derivable).
    """
    time_path = "/usr/bin/time"
    if not os.path.exists(time_path):
        return {}
    tmp = tempfile.NamedTemporaryFile(delete=False, mode="w+")
    tmp.close()
    try:
        cmd = [time_path, "-v", "-o", tmp.name, binary, *args]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        timing: Dict[str, Any] = {}

        def parse_elapsed_to_seconds(s: str):
            try:
                s = s.strip()
                if ":" not in s:
                    return float(s)
                parts = [p.strip() for p in s.split(":")]
                secs = float(parts[-1])
                mins = int(parts[-2]) if len(parts) >= 2 else 0
                hrs = int(parts[-3]) if len(parts) >= 3 else 0
                return hrs * 3600.0 + mins * 60.0 + secs
            except Exception:
                return None

        with open(tmp.name, "r") as f:
            for line in f:
                ls = line.strip()
                if ls.startswith("User time (seconds):"):
                    try:
                        timing["user_s"] = float(ls.split(":", 1)[1].strip())
                    except Exception:
                        pass
                elif ls.startswith("System time (seconds):"):
                    try:
                        timing["sys_s"] = float(ls.split(":", 1)[1].strip())
                    except Exception:
                        pass
                elif ls.startswith("Elapsed (wall clock) time"):
                    # Use rsplit to avoid splitting inside "h:mm:ss" text
                    val = ls.rsplit(":", 1)[1].strip()
                    ev = parse_elapsed_to_seconds(val)
                    if ev is not None:
                        timing["elapsed_s"] = ev
        # Derive wait time
        if all(k in timing for k in ("elapsed_s", "user_s", "sys_s")):
            timing["wait_time_s"] = max(0.0, timing["elapsed_s"] - (timing["user_s"] + timing["sys_s"]))
        return timing
    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass

def run_strace_summary(binary: str, args: list) -> Dict[str, Any]:
    """Run strace -f -c and parse syscall summary into structured JSON.
    Returns { syscalls: [...], total: {...} } or { raw: "..." } on parse issues.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False)
    tmp.close()
    try:
        subprocess.run(["strace", "-f", "-c", "-o", tmp.name, binary, *args],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with open(tmp.name, "r") as f:
            raw_lines = [ln.rstrip("\n") for ln in f]
        # Strip empties for easier parsing, but keep original for raw fallback
        lines = [ln for ln in raw_lines if ln.strip()]
        header_idx = next((i for i, l in enumerate(lines) if "syscall" in l.lower()), None)
        if header_idx is None:
            return {"raw": "\n".join(raw_lines)}
        rows = []
        total = {}

        def safe_float(s: str) -> float:
            try:
                return float(s)
            except Exception:
                return 0.0

        def safe_int(s: str) -> int:
            try:
                return int(s)
            except Exception:
                return 0

        for l in lines[header_idx + 1:]:
            ls = l.strip()
            lo = ls.lower()
            if set(ls) <= {"-", " "}:  # separator line of dashes
                continue
            parts = ls.split()
            if len(parts) < 5:
                continue
            # Layout: %time seconds usecs/call calls [errors] syscall
            pct_time = safe_float(parts[0])
            seconds = safe_float(parts[1])
            usecs_per_call = safe_float(parts[2])
            calls = safe_int(parts[3])
            # errors may be present; detect if next token is int
            idx = 4
            errors = 0
            if idx < len(parts) and parts[idx].isdigit():
                errors = safe_int(parts[idx]); idx += 1
            syscall = " ".join(parts[idx:]) if idx < len(parts) else ""
            # Handle the 'total' summary row which appears with 'total' as syscall name in some versions
            if syscall.lower() == "total":
                total = {
                    "pct_time": pct_time if pct_time else 100.0,
                    "seconds": seconds,
                    "usecs_per_call": usecs_per_call,
                    "calls": calls,
                    "errors": errors,
                }
                continue
            if not syscall:
                continue
            rows.append({
                "syscall": syscall,
                "calls": calls,
                "errors": errors,
                "seconds": seconds,
                "usecs_per_call": usecs_per_call,
                "pct_time": pct_time,
            })

        # Sort by seconds desc then calls desc for readability
        rows.sort(key=lambda r: (r.get("seconds", 0.0), r.get("calls", 0)), reverse=True)
        return {"syscalls": rows, "total": total} if rows else {"raw": "\n".join(raw_lines)}
    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass

def run_valgrind_memcheck(binary: str, args: list) -> Dict[str, Any]:
    """Run valgrind memcheck and parse leak summary and error count."""
    # Capture stderr where memcheck writes its summary
    proc = subprocess.run([
        "valgrind", "--tool=memcheck", "--leak-check=summary", "--track-origins=no",
        binary, *args
    ], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    out = proc.stderr.splitlines()
    res: Dict[str, Any] = {}
    for ln in out:
        s = ln.strip()
        if s.startswith("ERROR SUMMARY:"):
            # e.g., ERROR SUMMARY: 0 errors from 0 contexts (suppressed: 0 from 0)
            try:
                num = int(s.split(':',1)[1].strip().split()[0])
                res["errors"] = num
            except Exception:
                pass
        elif s.startswith("definitely lost:"):
            # definitely lost: 0 bytes in 0 blocks
            try:
                res["definitely_lost_bytes"] = int(s.split(':',1)[1].strip().split()[0].replace(',',''))
            except Exception:
                pass
        elif s.startswith("indirectly lost:"):
            try:
                res["indirectly_lost_bytes"] = int(s.split(':',1)[1].strip().split()[0].replace(',',''))
            except Exception:
                pass
        elif s.startswith("possibly lost:"):
            try:
                res["possibly_lost_bytes"] = int(s.split(':',1)[1].strip().split()[0].replace(',',''))
            except Exception:
                pass
        elif s.startswith("still reachable:"):
            try:
                res["still_reachable_bytes"] = int(s.split(':',1)[1].strip().split()[0].replace(',',''))
            except Exception:
                pass
        elif s.startswith("suppressed:"):
            try:
                res["suppressed_bytes"] = int(s.split(':',1)[1].strip().split()[0].replace(',',''))
            except Exception:
                pass
    return res
def run_max_rss_kb(binary: str, args: list) -> int | None:
    """Run the target once under /usr/bin/time -v and return Max RSS in KB.
    Returns None if /usr/bin/time is unavailable or parsing fails.
    """
    time_path = "/usr/bin/time"
    if not os.path.exists(time_path):
        return None
    tmp = tempfile.NamedTemporaryFile(delete=False, mode="w+")
    tmp.close()
    try:
        # Run with verbose time, directing its output to the tmp file
        cmd = [time_path, "-v", "-o", tmp.name, binary, *args]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with open(tmp.name, "r") as f:
            for line in f:
                if "Maximum resident set size" in line:
                    # Format: Maximum resident set size (kbytes): 12345
                    parts = line.strip().split(":", 1)
                    if len(parts) == 2:
                        try:
                            return int(parts[1].strip())
                        except ValueError:
                            return None
    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass
    return None

def run_valgrind(binary: str, args: list) -> Dict[str, Any]:
    """Run valgrind Massif and parse peak memory from the raw massif output.
    Returns keys with _bytes suffix and additional context (snapshot/time).
    """
    massif_out = tempfile.NamedTemporaryFile(delete=False)
    massif_out.close()
    cmd = [
        "valgrind",
        "--tool=massif",
        f"--massif-out-file={massif_out.name}",
        # Count mmap'd pages as heap to better reflect real usage
        "--pages-as-heap=yes",
        binary,
        *args,
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    peak = {
        "massif_peak_heap_bytes": 0,
        "massif_peak_heap_extra_bytes": 0,
        "massif_peak_total_bytes": 0,
        "massif_peak_stacks_bytes": 0,
        "massif_peak_time": None,
        "massif_peak_snapshot": None,
    }

    cur_heap = cur_extra = cur_stacks = 0
    cur_time = None
    cur_snap = None

    def consider():
        nonlocal peak, cur_heap, cur_extra, cur_stacks, cur_time, cur_snap
        total = cur_heap + cur_extra
        if total > peak["massif_peak_total_bytes"]:
            peak.update({
                "massif_peak_heap_bytes": cur_heap,
                "massif_peak_heap_extra_bytes": cur_extra,
                "massif_peak_total_bytes": total,
                "massif_peak_stacks_bytes": cur_stacks,
                "massif_peak_time": cur_time,
                "massif_peak_snapshot": cur_snap,
            })

    try:
        with open(massif_out.name) as f:
            for line in f:
                line = line.strip()
                if line.startswith("snapshot="):
                    # finish previous snapshot before resetting
                    if cur_snap is not None:
                        consider()
                    cur_snap = int(line.split("=", 1)[1])
                    cur_heap = cur_extra = cur_stacks = 0
                    cur_time = None
                elif line.startswith("time="):
                    # time units can vary (i/instruction by default). Keep raw value.
                    v = line.split("=", 1)[1]
                    try:
                        cur_time = int(v)
                    except ValueError:
                        try:
                            cur_time = float(v)
                        except ValueError:
                            cur_time = None
                elif line.startswith("mem_heap_B="):
                    cur_heap = int(line.split("=", 1)[1])
                elif line.startswith("mem_heap_extra_B="):
                    cur_extra = int(line.split("=", 1)[1])
                elif line.startswith("mem_stacks_B="):
                    cur_stacks = int(line.split("=", 1)[1])
            # final snapshot
            if cur_snap is not None:
                consider()
    finally:
        try:
            os.unlink(massif_out.name)
        except Exception:
            pass

    return peak

def main():
    parser = argparse.ArgumentParser(description="Python profiler using perf and valgrind.")
    parser.add_argument("binary", help="Path to binary to profile")
    parser.add_argument("-o", "--output", default="profile.json", help="Output JSON file ('.json' will be appended if missing)")
    # Parse known args so -o can appear before or after the binary. Remaining args go to the target binary.
    known_args, program_args = parser.parse_known_args()

    binary = known_args.binary
    
    # Check for required tools before execution
    missing_tools = []
    # perf presence and usability
    perf_ok = False
    perf_usable = False
    perf_note = None
    if shutil.which("perf"):
        try:
            rv = subprocess.run(["perf", "version"], capture_output=True, text=True, timeout=3)
            perf_ok = (rv.returncode == 0 and "perf version" in (rv.stdout or ""))
        except Exception:
            perf_ok = False
        if perf_ok:
            # Check kernel perf_event_paranoid level early and REQUIRE it to be exactly '1'.
            # Many systems set this to a restrictive value; per your request we only
            # proceed when it's 1.
            perf_paranoid_level = None
            try:
                with open("/proc/sys/kernel/perf_event_paranoid", "r") as pf:
                    perf_paranoid_level = pf.read().strip()
            except Exception:
                perf_paranoid_level = None

            # Enforce exact value of 1. If it's not 1, abort with a helpful message.
            if perf_paranoid_level is None:
                print("Error: unable to determine /proc/sys/kernel/perf_event_paranoid.\n"
                      "perf requires perf_event_paranoid == 1 to run unprivileged counters.", file=sys.stderr)
                sys.exit(1)

            try:
                if int(perf_paranoid_level) != 1:
                    print(f"Error: kernel perf_event_paranoid={perf_paranoid_level}.\n"
                          "This profiler requires perf_event_paranoid to be set to 1 to run perf counters unprivileged.\n"
                          "Set it as root with: sudo sh -c 'echo 1 > /proc/sys/kernel/perf_event_paranoid'\n"
                          "Or run with appropriate privileges.", file=sys.stderr)
                    sys.exit(1)
                # paranoid == 1, do a quick perf probe to confirm usability
                test = subprocess.run(["perf", "stat", "-e", "task-clock", "/bin/true"], capture_output=True, text=True, timeout=4)
                out = (test.stdout or "") + (test.stderr or "")
                if test.returncode == 0 and "task-clock" in out:
                    perf_usable = True
                else:
                    perf_usable = False
                    perf_note = f"perf probe failed despite perf_event_paranoid=1"
            except Exception:
                print("Error: unexpected failure while probing perf; ensure perf is installed and usable.", file=sys.stderr)
                sys.exit(1)
            try:
                test = subprocess.run(["perf", "stat", "-e", "task-clock", "/bin/true"], capture_output=True, text=True, timeout=4)
                out = (test.stdout or "") + (test.stderr or "")
                if test.returncode == 0 and "task-clock" in out:
                    perf_usable = True
                else:
                    perf_usable = False
            except Exception as e:
                perf_usable = False
        else:
            # present but version failed; treat as missing
            pass
    else:
        perf_ok = False

    if not perf_ok:
        missing_tools.append("perf")
    if not shutil.which("valgrind"):
        missing_tools.append("valgrind")
    if not shutil.which("strace"):
        missing_tools.append("strace")
    
    if missing_tools:
        print(f"Error: Missing required tools: {', '.join(missing_tools)}", file=sys.stderr)
        print("Please install them before running the profiler.", file=sys.stderr)
        sys.exit(1)
    # Ensure output ends with .json
    output_path = known_args.output
    if not output_path.lower().endswith(".json"):
        output_path = output_path + ".json"

    # Memory sections size
    mem_sections = None
    try:
        size_out = subprocess.check_output(["size", "-B", "-d", binary], text=True)
        lines = size_out.strip().splitlines()
        if len(lines) >= 2:
            fields = lines[-1].split()
            numeric_fields = []
            for f in fields:
                try:
                    int(f, 10)
                    numeric_fields.append(f)
                except ValueError:
                    if len(numeric_fields) == 4:
                        numeric_fields.append(f)
                        break
            if len(numeric_fields) >= 4:
                mem_sections = {
                    "text_bytes": int(numeric_fields[0]),
                    "data_bytes": int(numeric_fields[1]),
                    "bss_bytes": int(numeric_fields[2]),
                    "total_bytes": int(numeric_fields[3])
                }
    except Exception as e:
        mem_sections = None

    # Architecture
    import platform
    arch = None
    try:
        file_out = subprocess.check_output(["file", binary], text=True)
        if "," in file_out:
            arch = file_out.split(",")[1].strip()
        else:
            arch = file_out.strip()
    except Exception:
        arch = platform.machine()

    # Binary sizes
    unstripped_size = os.path.getsize(binary)
    stripped_size = None
    if shutil.which("strip"):
        import tempfile
        tmp_stripped = tempfile.NamedTemporaryFile(delete=False)
        tmp_stripped.close()
        subprocess.run(["strip", binary, "-o", tmp_stripped.name], check=True)
        stripped_size = os.path.getsize(tmp_stripped.name)
        os.unlink(tmp_stripped.name)

    perf_data = run_perf_stat(binary, program_args) if perf_usable else {}
    # Wait time: elapsed - (user + sys), clamped to non-negative
    wait_time = None
    try:
        if "elapsed_s" in perf_data and "user_s" in perf_data and "sys_s" in perf_data:
            wait_time = max(0.0, perf_data["elapsed_s"] - (perf_data["user_s"] + perf_data["sys_s"]))
    except Exception:
        wait_time = None

    # Functional grouping
    timing = {}
    if "elapsed_s" in perf_data: timing["elapsed_s"] = perf_data["elapsed_s"]
    if "user_s" in perf_data: timing["user_s"] = perf_data["user_s"]
    if "sys_s" in perf_data: timing["sys_s"] = perf_data["sys_s"]
    if wait_time is not None: timing["wait_time_s"] = wait_time
    if "task_clock_ms" in perf_data: timing["task_clock_ms"] = perf_data["task_clock_ms"]
    # CPU utilization derived from task-clock vs elapsed
    if "task_clock_ms" in timing and "elapsed_s" in timing and timing["elapsed_s"] > 0:
        util = 100.0 * (timing["task_clock_ms"] / (timing["elapsed_s"] * 1000.0))
        timing["cpu_utilization_pct"] = round(util, 3)
        try:
            cores = os.cpu_count() or 1
        except Exception:
            cores = 1
        timing["cpu_utilization_per_core_pct"] = round(util / cores, 3)
    # Fallback to /usr/bin/time -v to populate missing timing
    if "elapsed_s" not in timing or "user_s" not in timing or "sys_s" not in timing:
        tv = run_time_v_timing(binary, program_args)
        # only add if not present
        for k, v in tv.items():
            if k not in timing:
                timing[k] = v

    cpu = {}
    cache = {}
    memory_access = {}
    
    if perf_data:
        # Core CPU metrics
        if "instructions" in perf_data: cpu["instructions"] = perf_data["instructions"]
        if "cycles" in perf_data: cpu["cycles"] = perf_data["cycles"]
        if "ref_cycles" in perf_data: cpu["ref_cycles"] = perf_data["ref_cycles"]
        
        # IPC and frequency
        if "instructions" in perf_data and "cycles" in perf_data and perf_data["cycles"]:
            cpu["ipc"] = round(perf_data["instructions"] / perf_data["cycles"], 6)
        if "cycles" in perf_data and "ref_cycles" in perf_data and perf_data["ref_cycles"]:
            cpu["frequency_ratio"] = round(perf_data["cycles"] / perf_data["ref_cycles"], 6)
        
        # Pipeline stalls
        if "stalled_cycles_frontend" in perf_data:
            cpu["stalled_cycles_frontend"] = perf_data["stalled_cycles_frontend"]
            if "cycles" in perf_data and perf_data["cycles"]:
                cpu["frontend_stall_pct"] = round(100.0 * perf_data["stalled_cycles_frontend"] / perf_data["cycles"], 3)
        if "stalled_cycles_backend" in perf_data:
            cpu["stalled_cycles_backend"] = perf_data["stalled_cycles_backend"]
            if "cycles" in perf_data and perf_data["cycles"]:
                cpu["backend_stall_pct"] = round(100.0 * perf_data["stalled_cycles_backend"] / perf_data["cycles"], 3)
        
        # Branching
        if "branches" in perf_data: cpu["branches"] = perf_data["branches"]
        if "branch_misses" in perf_data: cpu["branch_misses"] = perf_data["branch_misses"]
        if "branches" in perf_data and "branch_misses" in perf_data and perf_data["branches"]:
            cpu["branch_miss_rate_pct"] = round(100.0 * perf_data["branch_misses"] / perf_data["branches"], 6)
        
        # Instruction rate
        if "instructions" in cpu and "elapsed_s" in timing and timing.get("elapsed_s"):
            cpu["instructions_per_second"] = int(cpu["instructions"] / timing["elapsed_s"])
        
        # L1 Data Cache - always include all fields
        cache["l1d_loads"] = perf_data.get("l1_dcache_loads")
        cache["l1d_load_misses"] = perf_data.get("l1_dcache_load_misses")
        cache["l1d_stores"] = perf_data.get("l1_dcache_stores")
        if perf_data.get("l1_dcache_loads") and perf_data.get("l1_dcache_load_misses"):
            rate = 100.0 * perf_data["l1_dcache_load_misses"] / max(1, perf_data["l1_dcache_loads"])
            cache["l1d_miss_rate_pct"] = round(min(rate, 100.0), 6)
        else:
            cache["l1d_miss_rate_pct"] = None
        
        # L1 Instruction Cache - always include all fields
        cache["l1i_loads"] = perf_data.get("l1_icache_loads")
        cache["l1i_load_misses"] = perf_data.get("l1_icache_load_misses")
        if perf_data.get("l1_icache_loads") and perf_data.get("l1_icache_load_misses"):
            rate = 100.0 * perf_data["l1_icache_load_misses"] / max(1, perf_data["l1_icache_loads"])
            cache["l1i_miss_rate_pct"] = round(min(rate, 100.0), 6)
        else:
            cache["l1i_miss_rate_pct"] = None
        
        # LLC (L3) - always include all fields
        cache["llc_loads"] = perf_data.get("llc_loads")
        cache["llc_load_misses"] = perf_data.get("llc_load_misses")
        cache["llc_stores"] = perf_data.get("llc_stores")
        cache["llc_store_misses"] = perf_data.get("llc_store_misses")
        if perf_data.get("llc_loads") and perf_data.get("llc_load_misses"):
            rate = 100.0 * perf_data["llc_load_misses"] / max(1, perf_data["llc_loads"])
            cache["llc_miss_rate_pct"] = round(min(rate, 100.0), 6)
        else:
            cache["llc_miss_rate_pct"] = None
        
        # TLB - always include all fields
        memory_access["dtlb_loads"] = perf_data.get("dtlb_loads")
        memory_access["dtlb_load_misses"] = perf_data.get("dtlb_load_misses")
        memory_access["itlb_loads"] = perf_data.get("itlb_loads")
        memory_access["itlb_load_misses"] = perf_data.get("itlb_load_misses")
        if perf_data.get("dtlb_loads") and perf_data.get("dtlb_load_misses"):
            rate = 100.0 * perf_data["dtlb_load_misses"] / max(1, perf_data["dtlb_loads"])
            memory_access["dtlb_miss_rate_pct"] = round(min(rate, 100.0), 6)
        else:
            memory_access["dtlb_miss_rate_pct"] = None
        if perf_data.get("itlb_loads") and perf_data.get("itlb_load_misses"):
            rate = 100.0 * perf_data["itlb_load_misses"] / max(1, perf_data["itlb_loads"])
            memory_access["itlb_miss_rate_pct"] = round(min(rate, 100.0), 6)
        else:
            memory_access["itlb_miss_rate_pct"] = None
        
        # Page faults breakdown - always include all fields
        memory_access["page_faults"] = perf_data.get("page_faults")
        memory_access["minor_faults"] = perf_data.get("minor_faults")
        memory_access["major_faults"] = perf_data.get("major_faults")
        
        # Alignment and emulation faults - always include
        memory_access["alignment_faults"] = perf_data.get("alignment_faults")
        memory_access["emulation_faults"] = perf_data.get("emulation_faults")
        
        # Enhanced memory bandwidth estimation - always include all fields
        # LLC misses represent traffic between cache and RAM (cache line = 64 bytes)
        cache_line_bytes = 64
        elapsed = timing.get("elapsed_s", 0)
        
        if elapsed > 0:
            # Read bandwidth from LLC load misses
            if perf_data.get("llc_load_misses") is not None:
                read_bytes = perf_data["llc_load_misses"] * cache_line_bytes
                memory_access["ram_read_bytes"] = read_bytes
                memory_access["ram_read_mb"] = round(read_bytes / (1024 * 1024), 3)
                memory_access["ram_read_bandwidth_mbps"] = round(read_bytes / (1024 * 1024 * elapsed), 3)
            else:
                memory_access["ram_read_bytes"] = None
                memory_access["ram_read_mb"] = None
                memory_access["ram_read_bandwidth_mbps"] = None
            
            # Write bandwidth from LLC store misses (if available)
            if perf_data.get("llc_store_misses") is not None:
                write_bytes = perf_data["llc_store_misses"] * cache_line_bytes
                memory_access["ram_write_bytes"] = write_bytes
                memory_access["ram_write_mb"] = round(write_bytes / (1024 * 1024), 3)
                memory_access["ram_write_bandwidth_mbps"] = round(write_bytes / (1024 * 1024 * elapsed), 3)
            else:
                memory_access["ram_write_bytes"] = None
                memory_access["ram_write_mb"] = None
                memory_access["ram_write_bandwidth_mbps"] = None
            
            # Total memory bandwidth (read + write)
            if perf_data.get("llc_load_misses") is not None and perf_data.get("llc_store_misses") is not None:
                total_bytes = (perf_data["llc_load_misses"] + perf_data["llc_store_misses"]) * cache_line_bytes
                memory_access["ram_total_bytes"] = total_bytes
                memory_access["ram_total_mb"] = round(total_bytes / (1024 * 1024), 3)
                memory_access["ram_total_bandwidth_mbps"] = round(total_bytes / (1024 * 1024 * elapsed), 3)
                
                # Percentage breakdown
                if total_bytes > 0:
                    memory_access["ram_read_pct"] = round(100.0 * (perf_data["llc_load_misses"] * cache_line_bytes) / total_bytes, 2)
                    memory_access["ram_write_pct"] = round(100.0 * (perf_data["llc_store_misses"] * cache_line_bytes) / total_bytes, 2)
                else:
                    memory_access["ram_read_pct"] = None
                    memory_access["ram_write_pct"] = None
            else:
                memory_access["ram_total_bytes"] = None
                memory_access["ram_total_mb"] = None
                memory_access["ram_total_bandwidth_mbps"] = None
                memory_access["ram_read_pct"] = None
                memory_access["ram_write_pct"] = None
            
            # Alternative: L1 data cache traffic (gives cache-level bandwidth, not RAM)
            # This shows total memory operations at L1 level (useful for comparison)
            if perf_data.get("l1_dcache_loads") is not None and perf_data.get("l1_dcache_stores") is not None:
                l1_traffic_bytes = (perf_data["l1_dcache_loads"] + perf_data["l1_dcache_stores"]) * cache_line_bytes
                memory_access["l1_cache_traffic_mb"] = round(l1_traffic_bytes / (1024 * 1024), 3)
                memory_access["l1_cache_bandwidth_mbps"] = round(l1_traffic_bytes / (1024 * 1024 * elapsed), 3)
            else:
                memory_access["l1_cache_traffic_mb"] = None
                memory_access["l1_cache_bandwidth_mbps"] = None
        else:
            # No elapsed time - set all bandwidth fields to None
            memory_access["ram_read_bytes"] = None
            memory_access["ram_read_mb"] = None
            memory_access["ram_read_bandwidth_mbps"] = None
            memory_access["ram_write_bytes"] = None
            memory_access["ram_write_mb"] = None
            memory_access["ram_write_bandwidth_mbps"] = None
            memory_access["ram_total_bytes"] = None
            memory_access["ram_total_mb"] = None
            memory_access["ram_total_bandwidth_mbps"] = None
            memory_access["ram_read_pct"] = None
            memory_access["ram_write_pct"] = None
            memory_access["l1_cache_traffic_mb"] = None
            memory_access["l1_cache_bandwidth_mbps"] = None
    else:
        # Provide a helpful note if perf wasn't usable
        if not perf_usable and perf_ok:
            # Try to read paranoid level for context
            level = None
            try:
                with open("/proc/sys/kernel/perf_event_paranoid", "r") as pf:
                    level = pf.read().strip()
            except Exception:
                level = None
            cpu["note"] = f"perf unusable (perf_event_paranoid={level})" if level is not None else "perf unusable (permission restricted)"

    # Detect number of threads by counting tasks in /proc during execution
    # We'll run a quick check to see how many threads the process spawns
    def count_threads(binary: str, args: list) -> int:
        """Count threads by running the binary and checking /proc/{pid}/task/"""
        try:
            proc = subprocess.Popen([binary, *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(0.01)  # Give it a moment to spawn threads
            task_dir = f"/proc/{proc.pid}/task"
            if os.path.exists(task_dir):
                thread_count = len(os.listdir(task_dir))
            else:
                thread_count = 1
            proc.terminate()
            proc.wait(timeout=1)
            return thread_count
        except Exception:
            return 1  # Default to single-threaded if we can't detect
    
    num_threads = count_threads(binary, program_args)
    
    concurrency = {}
    if perf_data:
        if "context_switches" in perf_data: concurrency["context_switches"] = perf_data["context_switches"]
        if "cpu_migrations" in perf_data: concurrency["cpu_migrations"] = perf_data["cpu_migrations"]
        # Context switching rate
        if "context_switches" in perf_data and "elapsed_s" in timing and timing.get("elapsed_s"):
            concurrency["ctx_switches_per_second"] = round(perf_data["context_switches"] / timing["elapsed_s"], 3)
        if "cpu_migrations" in perf_data and "elapsed_s" in timing and timing.get("elapsed_s"):
            concurrency["migrations_per_second"] = round(perf_data["cpu_migrations"] / timing["elapsed_s"], 3)
    concurrency["threads"] = num_threads

    # Syscalls summary via strace -c
    syscalls = run_strace_summary(binary, program_args)

    # Combine memory and memory_access into one section - always include all fields
    memory = {}
    # TLB stats - always present
    memory["dtlb_loads"] = memory_access.get("dtlb_loads")
    memory["dtlb_load_misses"] = memory_access.get("dtlb_load_misses")
    memory["dtlb_miss_rate_pct"] = memory_access.get("dtlb_miss_rate_pct")
    memory["itlb_loads"] = memory_access.get("itlb_loads")
    memory["itlb_load_misses"] = memory_access.get("itlb_load_misses")
    memory["itlb_miss_rate_pct"] = memory_access.get("itlb_miss_rate_pct")
    
    # Page faults - always present
    memory["page_faults"] = memory_access.get("page_faults")
    memory["minor_faults"] = memory_access.get("minor_faults")
    memory["major_faults"] = memory_access.get("major_faults")
    
    # Fault types - always present
    memory["alignment_faults"] = memory_access.get("alignment_faults")
    memory["emulation_faults"] = memory_access.get("emulation_faults")
    
    # RAM bandwidth (from LLC misses) - always present
    memory["ram_read_bytes"] = memory_access.get("ram_read_bytes")
    memory["ram_read_mb"] = memory_access.get("ram_read_mb")
    memory["ram_read_bandwidth_mbps"] = memory_access.get("ram_read_bandwidth_mbps")
    memory["ram_write_bytes"] = memory_access.get("ram_write_bytes")
    memory["ram_write_mb"] = memory_access.get("ram_write_mb")
    memory["ram_write_bandwidth_mbps"] = memory_access.get("ram_write_bandwidth_mbps")
    memory["ram_total_bytes"] = memory_access.get("ram_total_bytes")
    memory["ram_total_mb"] = memory_access.get("ram_total_mb")
    memory["ram_total_bandwidth_mbps"] = memory_access.get("ram_total_bandwidth_mbps")
    memory["ram_read_pct"] = memory_access.get("ram_read_pct")
    memory["ram_write_pct"] = memory_access.get("ram_write_pct")
    
    # L1 cache bandwidth (total memory operations) - always present
    memory["l1_cache_traffic_mb"] = memory_access.get("l1_cache_traffic_mb")
    memory["l1_cache_bandwidth_mbps"] = memory_access.get("l1_cache_bandwidth_mbps")
    
    # Max RSS from /usr/bin/time -v
    max_rss_kb = run_max_rss_kb(binary, program_args)
    if max_rss_kb is not None:
        memory["max_rss_kb"] = max_rss_kb
    
    # Valgrind Massif
    memory.update(run_valgrind(binary, program_args))
    
    # Valgrind Memcheck leak summary
    memcheck = run_valgrind_memcheck(binary, program_args)
    if memcheck:
        memory["memcheck"] = memcheck

    # --- Ensure stable schema: fill missing keys with explicit None defaults ---
    default_timing = {
        "elapsed_s": None,
        "user_s": None,
        "sys_s": None,
        "wait_time_s": None,
        "task_clock_ms": None,
        "cpu_utilization_pct": None,
        "cpu_utilization_per_core_pct": None,
    }

    default_cpu = {
        "instructions": None,
        "cycles": None,
        "ref_cycles": None,
        "ipc": None,
        "frequency_ratio": None,
        "stalled_cycles_frontend": None,
        "frontend_stall_pct": None,
        "stalled_cycles_backend": None,
        "backend_stall_pct": None,
        "branches": None,
        "branch_misses": None,
        "branch_miss_rate_pct": None,
        "instructions_per_second": None,
        "note": None,
    }

    default_cache = {
        "l1d_loads": None,
        "l1d_load_misses": None,
        "l1d_stores": None,
        "l1d_miss_rate_pct": None,
        "l1i_loads": None,
        "l1i_load_misses": None,
        "l1i_miss_rate_pct": None,
        "llc_loads": None,
        "llc_load_misses": None,
        "llc_stores": None,
        "llc_store_misses": None,
        "llc_miss_rate_pct": None,
    }

    default_concurrency = {
        "context_switches": None,
        "cpu_migrations": None,
        "ctx_switches_per_second": None,
        "migrations_per_second": None,
        "threads": num_threads if 'num_threads' in locals() else None,
    }

    default_memory = {
        # TLB
        "dtlb_loads": None,
        "dtlb_load_misses": None,
        "dtlb_miss_rate_pct": None,
        "itlb_loads": None,
        "itlb_load_misses": None,
        "itlb_miss_rate_pct": None,
        # Page faults
        "page_faults": None,
        "minor_faults": None,
        "major_faults": None,
        # Fault types
        "alignment_faults": None,
        "emulation_faults": None,
        # RAM bandwidth
        "ram_read_bytes": None,
        "ram_read_mb": None,
        "ram_read_bandwidth_mbps": None,
        "ram_write_bytes": None,
        "ram_write_mb": None,
        "ram_write_bandwidth_mbps": None,
        "ram_total_bytes": None,
        "ram_total_mb": None,
        "ram_total_bandwidth_mbps": None,
        "ram_read_pct": None,
        "ram_write_pct": None,
        # L1 cache traffic
        "l1_cache_traffic_mb": None,
        "l1_cache_bandwidth_mbps": None,
        # Max RSS
        "max_rss_kb": None,
        # Massif peaks
        "massif_peak_heap_bytes": None,
        "massif_peak_heap_extra_bytes": None,
        "massif_peak_total_bytes": None,
        "massif_peak_stacks_bytes": None,
        "massif_peak_time": None,
        "massif_peak_snapshot": None,
        # Memcheck
        "memcheck": None,
    }

    # Binary footprint defaults
    default_footprint = {
        "unstripped_bytes": None,
        "stripped_bytes": None,
        "sections": None,
    }

    # Ensure top-level dicts include all expected keys (fill with None when absent)
    ensure_keys(timing, default_timing)
    ensure_keys(cpu, default_cpu)
    ensure_keys(cache, default_cache)
    ensure_keys(concurrency, default_concurrency)
    ensure_keys(memory, default_memory)
    ensure_keys(result := {}, {})  # ensure result exists for later construction (no-op)

    # Apply defaults to binary footprint container
    # `unstripped_size` and `stripped_size` already computed; keep them but ensure keys exist
    binary_footprint = {
        "unstripped_bytes": unstripped_size if 'unstripped_size' in locals() else None,
        "stripped_bytes": stripped_size if 'stripped_size' in locals() else None,
        "sections": mem_sections,
    }
    ensure_keys(binary_footprint, default_footprint)

    # Ensure syscalls output is always a dict with either syscalls/total or raw
    if not isinstance(syscalls, dict):
        syscalls = {"raw": str(syscalls)}
    if "syscalls" not in syscalls and "raw" not in syscalls:
        syscalls = {"syscalls": [], "total": {}}

    result = {
        "binary": os.path.basename(binary),
        "architecture": arch,
        "binary_footprint": {
            "unstripped_bytes": unstripped_size,
            "stripped_bytes": stripped_size,
            "sections": mem_sections,
        },
        "timing": timing,
        "cpu": cpu,
        "cache": cache,
        "concurrency": concurrency,
        "syscalls": syscalls,
        "memory": memory
    }

    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()