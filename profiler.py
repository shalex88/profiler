#!/usr/bin/env python3

import argparse
import subprocess
import json
import tempfile
import os
import sys
import shutil
from typing import Dict, Any

def run_perf_stat(binary: str, args: list) -> Dict[str, Any]:
    cmd = ["perf", "stat", "-e",
           "task-clock,instructions,cycles,branches,branch-misses,cache-misses,context-switches,cpu-migrations,page-faults",
           binary] + args
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
        if "task-clock" in line:
            metrics["task_clock_ms"] = float(line.split()[0].replace(",", ""))
        elif "instructions" in line and "insn per cycle" in line:
            metrics["instructions"] = int(line.split()[0].replace(",", ""))
        elif "cycles" in line and "GHz" in line:
            metrics["cycles"] = int(line.split()[0].replace(",", ""))
        elif "branches" in line and "M/sec" in line:
            metrics["branches"] = int(line.split()[0].replace(",", ""))
        elif "branch-misses" in line:
            metrics["branch_misses"] = int(line.split()[0].replace(",", ""))
        elif "cache-misses" in line:
            metrics["cache_misses"] = int(line.split()[0].replace(",", ""))
        elif "context-switches" in line:
            metrics["context_switches"] = int(line.split()[0].replace(",", ""))
        elif "cpu-migrations" in line:
            metrics["cpu_migrations"] = int(line.split()[0].replace(",", ""))
        elif "page-faults" in line:
            metrics["page_faults"] = int(line.split()[0].replace(",", ""))
        elif "seconds time elapsed" in line:
            metrics["elapsed_s"] = float(line.strip().split()[0])
        elif "seconds user" in line:
            metrics["user_s"] = float(line.strip().split()[0])
        elif "seconds sys" in line:
            metrics["sys_s"] = float(line.strip().split()[0])
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
    if perf_data:
        if "instructions" in perf_data: cpu["instructions"] = perf_data["instructions"]
        if "cycles" in perf_data: cpu["cycles"] = perf_data["cycles"]
        if "instructions" in perf_data and "cycles" in perf_data and perf_data["cycles"]:
            cpu["ipc"] = round(perf_data["instructions"] / perf_data["cycles"], 6)
        if "branches" in perf_data: cpu["branches"] = perf_data["branches"]
        if "branch_misses" in perf_data: cpu["branch_misses"] = perf_data["branch_misses"]
        if "cache_misses" in perf_data: cpu["cache_misses"] = perf_data["cache_misses"]
        # Instruction rate
        if "instructions" in cpu and "elapsed_s" in timing and timing.get("elapsed_s"):
            cpu["instructions_per_second"] = int(cpu["instructions"] / timing["elapsed_s"])
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

    scheduling = {}
    if "context_switches" in perf_data: scheduling["context_switches"] = perf_data["context_switches"]
    if "cpu_migrations" in perf_data: scheduling["cpu_migrations"] = perf_data["cpu_migrations"]
    # Syscalls summary via strace -c
    strace_res = run_strace_summary(binary, program_args)
    if strace_res:
        scheduling["syscalls"] = strace_res

    memory = {}
    if "page_faults" in perf_data: memory["page_faults"] = perf_data["page_faults"]
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
        "scheduling": scheduling,
        "memory": memory
    }

    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()