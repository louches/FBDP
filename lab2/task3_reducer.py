#!/usr/bin/env python3
import sys
from collections import defaultdict

user_activity = defaultdict(int)  # {user_id: active_days}

for line in sys.stdin:
    line = line.strip()
    user_id, count = line.split("\t")
    user_activity[user_id] += int(count)

for user_id, active_days in sorted(user_activity.items(), key=lambda x: -x[1]):
    print(f"{user_id}\t{active_days}")
