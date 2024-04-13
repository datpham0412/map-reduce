from pymongo import MongoClient

database_name = 'music_data'
collection_name = 'songs'

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client[database_name]
collection = db[collection_name]

# Open a file to write the triplets
with open('task1_1_output.txt', 'w') as file:
    # Query to retrieve the fields
    for song in collection.find({}, {'Artist': 1, 'Year': 1, 'Sales': 1, '_id': 0}):
        # Create a triplet for each song
        triplet = f"{song['Artist']}, {song['Year']}, {song['Sales']}\n"
        # Write the triplet to a file
        file.write(triplet)
