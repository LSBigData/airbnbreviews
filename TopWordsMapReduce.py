from mrjob.job import MRJob
import re
import os

class TopWordsMapReduce(MRJob):

    def configure_args(self):
        super(TopWordsMapReduce, self).configure_args()
        self.add_passthru_arg('--stopwords', help='Path to stopwords file')

    def mapper_init(self):
        self.stopwords = set()
        if self.options.stopwords:
            dir_path = os.path.dirname(os.path.realpath(__file__))
            lines = os.path.join(dir_path, self.options.stopwords)
            with open(lines, 'r') as f:
                self.stopwords = set(line.strip() for line in f)

    def mapper(self, _, line):
        words = re.findall(r'\w+', line.lower())
        for word in words:
            if word not in self.stopwords:
                yield word, 1

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer_init(self):
        self.top_words = []

    def reducer(self, word, counts):
        total_count = sum(counts)
        self.top_words.append((total_count, word))
        if len(self.top_words) > 10:
            self.top_words.sort(reverse=True)
            self.top_words.pop()

    def reducer_final(self):
        self.top_words.sort(reverse=True)
        for count, word in self.top_words:
            yield word, count

if __name__ == '__main__':
    TopWordsMapReduce.run()
