#!/usr/bin/env python3
import sys
import argparse
import subprocess
import re
from collections import defaultdict

def main():
    parser = argparse.ArgumentParser(description="Parse perf.data to get inclusive cycles and instructions for a function and its children in the call tree.")
    parser.add_argument("-d", "--data", default="perf.data", help="Path to perf.data file")
    parser.add_argument("-f", "--filter", required=True, help="Function name to filter on (e.g., update_frontier)")
    args = parser.parse_args()

    cmd = ["sudo", "perf", "script", "-i", args.data, "-F", "event,period,ip,sym"]
    print(f"Reading '{args.data}' and filtering for '{args.filter}' (inclusive)...")
    
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except FileNotFoundError:
        print("Error: 'perf' command not found.")
        sys.exit(1)

    # regex for getting event and period
    header_re = re.compile(r'^\s+(\d+)\s+([^:]+).*:$')
    
    # regex for callchain lines
    callchain_re = re.compile(r'^\s+[0-9a-fA-F]+\s+(.+)$')

    totals = {"cycles": 0, "instructions": 0}
    children_stats = defaultdict(lambda: {"cycles": 0, "instructions": 0})
    
    current_event = None
    current_period = 0
    current_stack = []

    def commit_sample():
        nonlocal current_event, current_period, current_stack
        if not current_event or not current_stack:
            return
            
        # perf script default callchain order is Leaf -> Root (callee -> caller)
        # We find the deepest index (closest to root) that matches the filter,
        # so we include all its children.
        target_idx = -1
        for i, sym in enumerate(current_stack):
            if args.filter in sym:
                target_idx = i
                # Do not break, keep iterating to find the outermost occurrence in case of recursion
        
        if target_idx != -1:
            ev = current_event.lower()
            cycles = current_period if 'cycle' in ev else 0
            insn = current_period if ('instruction' in ev or 'insn' in ev) else 0
            
            totals["cycles"] += cycles
            totals["instructions"] += insn
            
            # Functions from leaf (0) up to target_idx-1 are children of our target.
            children = set(current_stack[:target_idx])
            for child in children:
                children_stats[child]["cycles"] += cycles
                children_stats[child]["instructions"] += insn

    for line in proc.stdout:
        line = line.rstrip()
        if not line:
            continue
            
        header_match = header_re.match(line)
        if header_match:
            commit_sample()
            current_period = int(header_match.group(1))
            current_event = header_match.group(2).strip()
            current_stack = []
            continue
            
        call_match = callchain_re.match(line)
        if call_match:
            sym = call_match.group(1)
            current_stack.append(sym)

    commit_sample()
    
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        print(f"perf script failed with error:\n{stderr}")
        sys.exit(1)

    cyc = totals["cycles"]
    ins = totals["instructions"]
    ipc = (ins / cyc) if cyc > 0 else 0.0

    print(f"\n--- Inclusive Profile for '{args.filter}' and its Called Children ---")
    print(f"{'Function Name':<40} | {'Cycles':<15} | {'Instructions':<15} | {'IPC':<5} | {'% of Target Cycles'}")
    print("-" * 108)
    print(f"{'<Target Overall>':<40} | {cyc:<15,} | {ins:<15,} | {ipc:<5.2f} | 100.00%")
    print("-" * 108)
    
    if children_stats:
        # Sort children by cycles descending
        sorted_children = sorted(children_stats.items(), key=lambda x: x[1]["cycles"], reverse=True)
        for child, stats in sorted_children:
            ccyc = stats["cycles"]
            cins = stats["instructions"]
            cipc = (cins / ccyc) if ccyc > 0 else 0.0
            pct = (ccyc / cyc * 100) if cyc > 0 else 0.0
            
            # Clamp long symbol names
            cname = child if len(child) <= 40 else child[:37] + "..."
            print(f"{cname:<40} | {ccyc:<15,} | {cins:<15,} | {cipc:<5.2f} | {pct:>5.2f}%")
    else:
        print("No children found (or target function is always a leaf).")

if __name__ == "__main__":
    main()
