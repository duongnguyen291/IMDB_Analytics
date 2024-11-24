class IMDbDataLoader:
    def __init__(self, spark, base_path):
        self.spark = spark
        self.base_path = base_path
    
    def load_titles(self):
        return self.spark.read.parquet(f"{self.base_path}/title_basics_parquet") # basic in4 about titles
    
    def load_ratings(self):
        return self.spark.read.parquet(f"{self.base_path}/title_ratings_parquet") # in4 about ratings and vote counts for titles
    
    def load_names(self):
        return self.spark.read.parquet(f"{self.base_path}/name_basics_parquet") # Basic in4 about individuals

    def load_akas(self):
        return self.spark.read.parquet(f"{self.base_path}/title_akas_parquet") # In4 about alternative titles of movies or shows
        
    def load_episodes(self):
        return self.spark.read.parquet(f"{self.base_path}/episode_parquet") # About episodoes in a series

    def load_principals(self):
        return self.spark.read.parquet(f"{self.base_path}/title_principals_parquet") # In4 about key indivisuals related to a title
    
    def load_crews(self):
        return self.spark.read.parquet(f"{self.base_path}/title_crew_parquet") # In4 about the creative team behind the film

