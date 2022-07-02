from mrjob.job import MRJob
from mrjob.step import MRStep

"""
Name: Manoah Tervoort
Student number: 643622
"""
class RatingsBreakdown (MRJob):
    # Define the steps that MRJob should run
    def steps(self):
        return [
            MRStep(
              mapper=self.mapper_get_movie_ratings,
              combiner=self.combiner_count_ratings,
              reducer=self.reducer_sort_by_ratings
            ),
        ]

    # Grade 6: Map the data to key,value pairs
    def mapper_get_movie_ratings(self, _, line):
        # Read the csv file and convert each line to its corresponding variables
        (_, movie_id, _, _) = line.split('\t')

        # Return a key,value pair of movie_id and nr of ratings
        yield movie_id, 1

    # Grade 6: Receives the key,value pairs with unique keys
    def combiner_count_ratings(self, movie_id, ratings):
        # Return a tuple with movie_id and the total nr of ratings 
        yield None, (sum(ratings), movie_id)

    # Grade 8: Sort the data by total nr of ratings
    def reducer_sort_by_ratings(self, _, movie_rating_counts):
      # Sort the tuple by nr_of_ratings in descending order
      for nr_of_ratings, movie_id in sorted(movie_rating_counts, reverse=True):
        # Return a key,value pair with movie id and total number of ratings
        yield int(movie_id), int(nr_of_ratings)

# Execute the program 
if __name__ == '__main__':
    RatingsBreakdown.run()


