#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    fields = line.split(',')
    if len(fields) < 2:
        continue
    try:
        date, flow_in, flow_out = fields[0], int(fields[1]), int(fields[2])
        interest_rate = float(fields[3])  # 示例假设为隔夜利率
        rate_group = "<2%" if interest_rate < 2 else "2%-4%" if interest_rate <= 4 else ">4%"
        print(f"{rate_group}\t{flow_in},{flow_out}")
    except ValueError:
        continue
