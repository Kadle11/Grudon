#!/usr/bin/env python3
import sys
import argparse
import subprocess
import re
from collections import defaultdict

def main():
    parser = argparse.ArgumentParser(description="Parse multiple perf.data files, average them, and get inclusive stats for a function's children.")
    parser.add_argument("-d", "--data", nargs='+', required=True, help="Paths to perf.data files (e.g., run1.data run2.data run3.data)")
    parser.add_argument("-f", "--filter", required=True, help="Function name to filter on (e.g., update_frontier)")
    args = parser.parse_args()

    num_files = len(args.data)
    if num_files == 0:
        print("No data files provided.")
        sys.exit(1)

    print(f"Filtering for '{args.filter}' across {num_files} files...")

    header_re = re.compile(r'^\s+(\d+)\s+([^:]+).*:$')
    callchain_re = re.compile(r'^\s+[0-9a-fA-F]+\s+(.+)$')

    totals = {"cycles": 0, "instructions": 0}
    children_stats = defaultdict(lambda: {"cycles": 0, "instructions": 0})

    for datafile in args.data:
        print(f"  -> Reading '{datafile}'...")
        cmd = ["sudo", "perf", "script", "-i", datafile, "-F", "event,period,ip,sym"]
        
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except FileNotFoundError:
            print("Error: 'perf' command not found.")
            sys.exit(1)

        current_event = None
        current_period = 0
        current_stack = []

        def commit_sample():
            nonlocal current_event, current_period, current_stack
            if not current_event or not current_stack:
                return
                
            target_idx = -1
            for i, sym in enumerate(current_stack):
                if args.filter in sym:
                    target_idx = i
            
            if target_idx != -1:
                ev = current_event.lower()
                cycles = current_period if 'cycle' in ev else 0
                insn = current_period if ('instruction' in ev or 'insn' in ev) else 0
                
                totals["cycles"] += cycles
                totals["instructions"] += insn
                
                children = set(current_stack[:target_idx])
                for child in children:
                    children_stats[child]["cycles"] += cycles
                    children_stats[child]["instructions"] += insn

        for line in proc.stdout:
            line = line.rstrip()
            if not line: continue
                
            header_match = header_re.match(line)
            if header_match:
                commit_sample()
                current_period = int(header_match.group(1))
                current_event = header_match.group(2).strip()
                current_stack = []
                continue
                
            call_match = callchain_re.match(line)
            if call_match:
                current_stack.append(call_match.group(1))

        commit_sample()
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            print(f"perf script failed on {datafile} with error:\n{stderr}")
            sys.exit(1)

    print("\nProcessing complete. Calculating averages...\n")

    # Calculate averages
    cyc = totals["cycles"] / num_files
    ins = totals["instructions"] / num_files
    ipc = (ins / cyc) if cyc > 0 else 0.0

    for child in children_stats:
        children_stats[child]["cycles"] /= num_files
        children_stats[child]["instructions"] /= num_files

    print(f"--- Averaged Inclusive Profile for '{args.filter}' ({num_files} runs) ---")
    print(f"{'<Target Overall>':<40} | Cycles: {cyc:,.0f} | Instructions: {ins:,.0f} | IPC: {ipc:.2f}")
    
    if not children_stats:
        print("No children found (or target function is always a leaf).")
        return

    def print_top(sort_key, title):
        print(f"\n--- Top 10 Contributors by Averaged {title} ---")
        base_event_total = cyc if sort_key == "cycles" else ins
        if base_event_total == 0:
            print(f"No {sort_key} recorded.")
            return

        print(f"{'Function Name':<40} | {'Avg Cycles':<15} | {'Avg Instructions':<16} | {'IPC':<5} | {'% of Target'}")
        print("-" * 105)
        
        sorted_children = sorted(children_stats.items(), key=lambda x: x[1][sort_key], reverse=True)[:10]
        for child, stats in sorted_children:
            ccyc = stats["cycles"]
            cins = stats["instructions"]
            cipc = (cins / ccyc) if ccyc > 0 else 0.0
            
            val_for_key = ccyc if sort_key == "cycles" else cins
            pct = (val_for_key / base_event_total * 100)
            
            cname = child if len(child) <= 40 else child[:37] + "..."
            print(f"{cname:<40} | {ccyc:<15,.0f} | {cins:<16,.0f} | {cipc:<5.2f} | {pct:>5.2f}%")

    print_top("cycles", "Cycles")
    print_top("instructions", "Instructions")

if __name__ == "__main__":
    main()
