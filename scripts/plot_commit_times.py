#!/usr/bin/env python3
"""
Plot commit time evolution from a Neo4j import log file.
Usage: python plot_commit_times.py <path_to_log_file>
"""

import sys
import re
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

def parse_log(filepath):
    """Parse committed batch lines from the log file, grouped by phase."""
    pattern = re.compile(
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+).*'
        r'\[(PASS \d+) Consumer\] Committed batch (\d+) \((\d+) articles total\)'
    )

    commits_by_phase = {}
    with open(filepath, 'r') as f:
        for line in f:
            m = pattern.search(line)
            if m:
                ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S.%f")
                phase = m.group(2)
                batch_num = int(m.group(3))
                articles = int(m.group(4))
                commits_by_phase.setdefault(phase, []).append((ts, batch_num, articles))

    return commits_by_phase


def compute_deltas(commits):
    """Compute time delta (seconds) between consecutive commits."""
    rows = []
    deltas = []
    for i in range(1, len(commits)):
        prev_ts = commits[i - 1][0]
        curr_ts = commits[i][0]
        delta = (curr_ts - prev_ts).total_seconds()
        articles = commits[i][2]
        rows.append(articles)
        deltas.append(delta)
    return rows, deltas


def plot(rows, deltas, filepath, phase_label):
    fig, ax = plt.subplots(figsize=(14, 6))

    # Plot line + markers
    ax.plot(rows, deltas, color='#2563eb', linewidth=1.5, alpha=0.8, zorder=3)
    ax.scatter(rows, deltas, color='#2563eb', s=8, zorder=4, alpha=0.7)

    # Average line
    avg = sum(deltas) / len(deltas)
    ax.axhline(avg, color='#ef4444', linewidth=1.0, linestyle='--', alpha=0.8, label=f'Avg: {avg:.1f}s')

    # Grid
    ax.grid(color='#e5e7eb', linestyle='--', linewidth=0.6)
    ax.set_axisbelow(True)

    # Axis labels & title
    ax.set_xlabel('Rows written (articles)', fontsize=11, labelpad=8)
    ax.set_ylabel('Time between commits (seconds)', fontsize=11, labelpad=8)
    ax.set_title(f'Neo4j Import — Commit Time Evolution ({phase_label})', fontsize=13, fontweight='bold', pad=12)

    # Tick formatting
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{int(x):,}'))
    ax.tick_params(labelsize=10)

    ax.legend(fontsize=10)

    plt.tight_layout()

    phase_suffix = phase_label.lower().replace(' ', '_')
    out_path = filepath.rsplit('.', 1)[0] + f'_{phase_suffix}_commit_times.png'
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    print(f"Plot saved to: {out_path}")
    plt.show()


def main():
    if len(sys.argv) < 2:
        print("Usage: python plot_commit_times.py <path_to_log_file>")
        sys.exit(1)

    filepath = sys.argv[1]
    print(f"Parsing log file: {filepath}")

    commits_by_phase = parse_log(filepath)
    if not commits_by_phase:
        print("No commit lines found. Check your log file.")
        sys.exit(1)

    total_commits = sum(len(commits) for commits in commits_by_phase.values())
    print(f"Found {total_commits} commit events across {len(commits_by_phase)} phase(s).")

    for phase_label in sorted(commits_by_phase.keys()):
        commits = commits_by_phase[phase_label]
        if len(commits) < 2:
            print(f"Skipping {phase_label}: not enough commit lines found (found {len(commits)}).")
            continue

        rows, deltas = compute_deltas(commits)

        print(f"\n{phase_label} stats:")
        print(f"  Min delta : {min(deltas):.2f}s")
        print(f"  Max delta : {max(deltas):.2f}s")
        print(f"  Avg delta : {sum(deltas)/len(deltas):.2f}s")

        plot(rows, deltas, filepath, phase_label)


if __name__ == '__main__':
    main()