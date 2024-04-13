from mrjob.job import MRJob
from mrjob.step import MRStep

class TopSellingArtistByYear(MRJob):
    
    def parse_data(self, data):
        # Split data string which contains the artist and year information
        artist, year = data.strip('[]"').split('", "')
        return artist, year
    
    def mapper(self, _, line):
        # Split the input line into artist_year and sales parts.
        data_sales = line.strip().split('\t')
        data = data_sales[0].strip('"')
        sales = float(data_sales[1].strip('"'))
        artist, year = self.parse_data(data)
        # Clean up year to remove any surrounding quotes
        year = year.strip('"')
        yield year, (artist, sales)
        
    def reducer_init(self):
        self.top_artist_by_year = {}
    
    def reducer(self, year, values):
        # Find the top-selling artist for the year
        max_sales = 0
        top_artist = None
        for artist, sales in values:
            if sales > max_sales:
                max_sales = sales
                top_artist = (artist, sales)
        # Store the top artist and sales for the year
        if top_artist:
            self.top_artist_by_year[year] = top_artist
    
    def reducer_final(self):
        # Emit all the years with top artists, which will be sorted in the next step
        for year, top_artist in sorted(self.top_artist_by_year.items(), key=lambda x: x[0], reverse=True):
            sort_key = str(9999 - int(year)).zfill(4)
            # Emitting a tuple (sort_key, year) as the key and (artist, sales) as the value
            yield (sort_key, year), top_artist
    
    def reducer_sort(self, sort_key_year, values):
        # sort_key_year is a tuple (sort_key, year)
        _, year = sort_key_year
        for artist_sales in values:
            # artist_sales is a tuple (artist, sales)
            artist, sales = artist_sales
            # Emit year and a list of [artist, sales]
            yield year, [artist, sales]


    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer,
                   reducer_final=self.reducer_final),
            #Sort years in descending order
            MRStep(reducer=self.reducer_sort)
        ]

if __name__ == '__main__':
    TopSellingArtistByYear().run()
    
# Command to compile the file:
# python .\task2_1.py task1_2_output.txt > task2_1_output.txt
