#!/usr/bin/env python3
import sys
from collections import defaultdict

flow_data = defaultdict(lambda: [0, 0])  # {date: [total_purchase_amt, total_redeem_amt]}

for line in sys.stdin:
    line = line.strip()
    date, amounts = line.split("\t")
    purchase, redeem = map(int, amounts.split(","))
    flow_data[date][0] += purchase
    flow_data[date][1] += redeem

for date in sorted(flow_data.keys()):
    print(f"{date}\t{flow_data[date][0]},{flow_data[date][1]}")
