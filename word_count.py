import re
from mrjob.job import MRJob
from mrjob.step import MRStep

class WordCount(MRJob):

    # 加载停词表
    def configure_args(self):
        super(WordCount, self).configure_args()
        self.add_file_arg('--stop-words')

    def mapper_init(self):
        with open(self.options.stop_words, 'r') as f:
            self.stop_words = set(word.strip().lower() for word in f)

    def mapper(self, _, line):
        columns = line.split(',')
        if len(columns) >= 2:
            headline = columns[1].strip().lower()
            words = re.findall(r'\b\w+\b', headline)
            for word in words:
                if word not in self.stop_words:
                    yield word, 1

    def reducer(self, key, values):
        yield None, (sum(values), key)

    def reducer_sort(self, _, word_counts):
        sorted_counts = sorted(word_counts, reverse=True, key=lambda x: x[0])
        for rank, (count, word) in enumerate(sorted_counts[:100], 1):
            yield f"{rank}：{word}", count

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_sort)
        ]

if __name__ == '__main__':
    WordCount.run()
