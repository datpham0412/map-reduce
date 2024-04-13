from mrjob.job import MRJob

class TotalSalesByArtistYear(MRJob):
    
    def mapper(self, _, line):
        try:
            artist, year, sales = line.split(',')
            yield (artist.strip(), year.strip()), float(sales.strip())
        except ValueError:
            pass

    def reducer(self, key, values):
        total_sales = sum(values)
        yield key, "{}".format(total_sales)

if __name__ == '__main__':
    TotalSalesByArtistYear().run()

#Command to compile the file:
#python .\task1_2.py task1_1_output.txt > task1_2_output.txt