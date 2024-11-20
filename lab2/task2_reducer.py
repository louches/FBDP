#!/usr/bin/env python3
import sys
from collections import defaultdict

flow_data = defaultdict(lambda: [0, 0, 0])  # {weekday: [total_in, total_out, count]}

for line in sys.stdin:
    line = line.strip()
    weekday, amounts = line.split("\t")
    flow_in, flow_out = map(int, amounts.split(","))
    flow_data[weekday][0] += flow_in
    flow_data[weekday][1] += flow_out
    flow_data[weekday][2] += 1

# Calculate averages and sort by total flow_in
results = []
for weekday, (total_in, total_out, count) in flow_data.items():
    avg_in = total_in // count
    avg_out = total_out // count
    results.append((avg_in, weekday, avg_in, avg_out))

for _, weekday, avg_in, avg_out in sorted(results, reverse=True):
    print(f"{weekday}\t{avg_in},{avg_out}")
