class MovieAnalyzer:
    def __init__(self, movies_df, ratings_df):
        self.movies_df = movies_df
        self.ratings_df = ratings_df
    # 
    def get_genre_distribution(self):
        return self.movies_df.select(
            explode(split("genres", ",")).alias("genre")
        ).groupBy("genre").count()
    
    def get_rating_distribution(self):
        return self.ratings_df.select(
            when(col("averageRating") >= 9, "9-10")
            .when(col("averageRating") >= 8, "8-9")
            .otherwise("<8").alias("rating_range")
        ).groupBy("rating_range").count()