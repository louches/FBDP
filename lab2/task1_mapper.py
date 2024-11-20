#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    fields = line.split(',')
    if len(fields) < 11:
        continue
    try:
        report_date = fields[1]
        total_purchase_amt = int(fields[4]) if fields[4] else 0
        total_redeem_amt = int(fields[8]) if fields[8] else 0
        print(f"{report_date}\t{total_purchase_amt},{total_redeem_amt}")
    except ValueError:
        continue
