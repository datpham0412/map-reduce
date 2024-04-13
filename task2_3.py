from mrjob.job import MRJob
from mrjob.step import MRStep

class TopSellingArtistsByDecade(MRJob):

    def mapper(self, _, line):
        # Parse the line into artist, year, and sales
        artist_year, sales = line.strip().split('\t')
        artist, year = artist_year.strip('[]"').split('", "')
        year = int(year)

        # Determine the decade for the given year
        decade = (year // 10) * 10
        yield (decade, artist), float(sales.strip('"'))

    def combiner(self, artist_decade, sales):
        # Combine sales for each artist and decade
        yield artist_decade, sum(sales)

    def reducer(self, artist_decade, sales):
        # Sum sales for each artist and decade and emit
        yield artist_decade[0], (sum(sales), artist_decade[1])

    def reducer_group_by_decade(self, decade, artist_sales):
        # Aggregate all artists' sales for the decade and sort them
        sorted_artists = sorted(artist_sales, reverse=True)[:3]  # Get top 3 artists
        # Format the output: ['Artist', sales]
        formatted_output = [[artist, '{:.3f}'.format(sales)] for sales, artist in sorted_artists]
        yield str(decade), formatted_output

    def reducer_sort_decades(self, decade, top_artists_lists):
        # Yield the decades in descending order
        # Negative decade for sorting in descending order
        yield int(decade) * -1, list(top_artists_lists)

    def reducer_output(self, neg_decade, top_artists_lists):
        # Convert the negative decade back to positive and format it.
        decade = -neg_decade
        decade_str = f"{decade}-{decade+9}"

        # Only output the top artists list for each decade in the desired format.
        # Yield each artist and sales as separate lines.
        for top_artists in top_artists_lists:
            for artist, sales in top_artists[0]:
                yield decade_str, [artist, sales]



    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            ),
            MRStep(
                reducer=self.reducer_group_by_decade
            ),
            MRStep(
                reducer=self.reducer_sort_decades
            ),
            MRStep(
                reducer=self.reducer_output
            )
        ]

if __name__ == '__main__':
    TopSellingArtistsByDecade.run()
