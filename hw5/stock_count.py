from mrjob.job import MRJob # type: ignore
from mrjob.step import MRStep

class StockCount(MRJob):

    def mapper(self, _, line):
        columns = line.split(',')
        if len(columns) >= 4:
            stock_code = columns[3].strip()
            yield stock_code, 1

    def reducer(self, key, values):
        yield None, (sum(values), key)

    def reducer_sort(self, _, stock_counts):
        # 排序并输出
        sorted_counts = sorted(stock_counts, reverse=True, key=lambda x: x[0])
        for rank, (count, stock_code) in enumerate(sorted_counts, 1):
            yield f"{rank}：{stock_code}", count

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_sort)
        ]

if __name__ == '__main__':
    StockCount.run()

