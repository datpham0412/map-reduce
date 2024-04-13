from mrjob.job import MRJob
from mrjob.step import MRStep

class TopSellingArtists(MRJob):

    def mapper(self, _, line):
        # Remove unwanted characters and split by tabs to separate artist-year and sales
        artist_year, sales = line.strip().split('\t')
        # Further parse the artist_year to extract the artist
        artist = artist_year.strip('[]"').split('", "')[0]
        # Convert sales to float and yield artist and sales
        yield artist, float(sales.strip('"'))

    def combiner(self, artist, sales):
        # Aggregate sales for each artist (combiner optimization)
        yield artist, sum(sales)

    def reducer(self, artist, sales):
        # Sum the total sales for each artist and emit
        yield None, (sum(sales), artist)

    def reducer_find_top(self, _, artist_sales):
        # Sort the artists by total sales and emit top 5
        top_artists = sorted(artist_sales, reverse=True)[:5]
        for total_sales, artist in top_artists:
            yield artist, total_sales

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_find_top)
        ]

if __name__ == '__main__':
    TopSellingArtists.run()
    
#Command to compile the file:
#python .\task2_2.py task1_2_output.txt > task2_2_output.txt