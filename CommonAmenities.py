from mrjob.job import MRJob
import re

class CommonAmenities(MRJob):

    def configure_args(self):
        super(CommonAmenities, self).configure_args()
        self.add_passthru_arg('--input-format', default='text')

    def mapper(self, _, line):
        if not line.startswith('location'):  # Skip the header line
            columns = line.split(',')
            location = columns[0]
            amenities_list = re.findall(r'\w+', columns[1].lower())
            for amenity in amenities_list:
                yield (location, amenity), 1

    def combiner(self, key, values):
        total_count = sum(values)
        yield key, total_count

    def reducer(self, key, values):
        total_count = sum(values)
        yield key, total_count

if __name__ == '__main__':
    CommonAmenities.run()