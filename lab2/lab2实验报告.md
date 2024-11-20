# lab2实验报告

#### 实验背景

蚂蚁金服每天处理海量资金流动数据，精准预测资金流入和流出情况对于优化流动性管理至关重要。本实验基于用户基本信息、交易记录、收益率和银行利率等多维度数据，利用 MapReduce 进行数据统计和分析，探索资金流动规律及其影响因素。

#### 实验环境

操作系统：Linux (Ubuntu 22.04)

Hadoop 版本：3.4.0

编程语言：Python 3

### **实验任务 1：每日资金流入流出统计**

#### **任务描述**

统计每个日期的总资金流入（`total_purchase_amt`）与总资金流出（`total_redeem_amt`）。处理缺失值，将其视为零交易。

#### **设计思路**

- **Mapper**：提取交易记录中的日期和资金流入、流出字段，将其按日期分组。
- **Reducer**：汇总每个日期的总资金流入和流出。

#### **代码实现**

**Mapper：`task1_mapper.py`**

```
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

```

**Reducer：`task1_reducer.py`**

```
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
```

#### **运行结果**

样例输出

```
20130701   32488348,5525022
20130702   29037390,2554548
20130703   27270770,5953867
20130704   18321185,6410729
20130705   11648749,2763587
```

### **实验任务 2：星期交易量统计**

#### **任务描述**

统计一周七天中每天的平均资金流入与流出量，并按照资金流入量从大到小排序。

#### **设计思路**

- **Mapper**：根据日期计算星期几，并输出对应的资金流入和流出。
- **Reducer**：统计每个星期的总资金流入和流出及交易天数，计算平均值。

#### **代码实现**

**Mapper：`task2_mapper.py`**

```
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

```

**Reducer：`task2_reducer.py`**

```
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
```

#### **运行结果**

```
Tuesday	263582058,191769144
Monday	260305810,217463865
Wednesday	254162607,194639446
Thursday	236425594,176466674
Friday	199407923,166467960
Sunday	155914551,132427205
Saturday	148088068,112868942
```

### **实验任务 3：用户活跃度分析**

#### **任务描述**

统计每个用户的活跃天数（当日有直接购买或赎回行为时视为活跃）。

#### **设计思路**

- **Mapper**：提取用户 ID 和日期，判断是否活跃。
- **Reducer**：统计每个用户的活跃天数并排序。

#### **代码实现**

**Mapper：`task3_mapper.py`**

```
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
```

**Reducer：`task3_reducer.py`**

```
import sys
from collections import defaultdict

user_activity = defaultdict(int)  # {user_id: active_days}

for line in sys.stdin:
    line = line.strip()
    user_id, count = line.split("\t")
    user_activity[user_id] += int(count)

for user_id, active_days in sorted(user_activity.items(), key=lambda x: -x[1]):
    print(f"{user_id}\t{active_days}")
```

#### **运行结果**

样例输出：

```
7629	384
11818	359
21723	334
19140	332
24378	315
26395	297
25147	295
27719	293
```

### **实验任务 4：交易行为影响因素分析**

#### **任务描述**

分析银行利率对申购和赎回行为的影响。

#### **设计思路**

- **Mapper**：提取日期和资金流入、流出，并按利率区间分组。
- **Reducer**：统计每个利率区间的日均资金流入和流出。

#### **代码实现**

**Mapper：`task4_mapper.py`**

```
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
```

**Reducer：`task4_reducer.py`**

```
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
```

#### **运行结果**

样例输出：

```
<2%	20138477,130269
2%-4%	20139194,1422
>4%	20139130,1907110
```

#### **结果分析**

- **低利率（<2%）**：流出资金较多，说明用户在低收益环境下倾向于减少投资。
- **中等利率（2%-4%）**：资金流动最稳定。
- **高利率（>4%）**：流入和流出资金都较大，说明高利率促进资金活跃度。



## 可能的改进之处

忘记本课程应该是使用java语言进行编程，下次实验使用java代替python语言