#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    fields = line.split(',')
    if len(fields) < 11:
        continue
    try:
        user_id = fields[0]
        direct_purchase = int(fields[5]) if fields[5] else 0
        total_redeem = int(fields[8]) if fields[8] else 0
        if direct_purchase > 0 or total_redeem > 0:
            print(f"{user_id}\t1")
    except ValueError:
        continue
