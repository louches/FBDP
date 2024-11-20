#!/usr/bin/env python3
import sys
from datetime import datetime

for line in sys.stdin:
    line = line.strip()
    fields = line.split("\t")
    if len(fields) != 2:
        continue
    try:
        date = fields[0]
        flow_in, flow_out = map(int, fields[1].split(","))
        weekday = datetime.strptime(date, "%Y%m%d").strftime("%A")
        print(f"{weekday}\t{flow_in},{flow_out}")
    except ValueError:
        continue
