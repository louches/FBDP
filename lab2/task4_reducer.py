#!/usr/bin/env python3
import sys
from collections import defaultdict

rate_data = defaultdict(lambda: [0, 0, 0])  # {rate_group: [total_in, total_out, count]}

for line in sys.stdin:
    line = line.strip()
    rate_group, amounts = line.split("\t")
    flow_in, flow_out = map(int, amounts.split(","))
    rate_data[rate_group][0] += flow_in
    rate_data[rate_group][1] += flow_out
    rate_data[rate_group][2] += 1

for rate_group, (total_in, total_out, count) in sorted(rate_data.items()):
    avg_in = total_in // count
    avg_out = total_out // count
    print(f"{rate_group}\t{avg_in},{avg_out}")
