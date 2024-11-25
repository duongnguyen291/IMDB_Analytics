from pyspark.sql.functions import col, explode, split, when, count, collect_set, desc
from pyspark.sql.window import Window

class MovieAnalyzer:
    def __init__(self, movies_df, crew_df, principals_df):
        """
        Initialize with three dataframes:
        - movies_df: Contains movie information
        - crew_df: Contains directors and writers information
        - principals_df: Contains detailed crew information (from the first table shown)
        """
        self.movies_df = movies_df
        self.crew_df = crew_df
        self.principals_df = principals_df
        
    def get_genre_distribution(self):
        """Analyze movie genre distribution"""
        return self.movies_df.select(
            explode(split("genres", ",")).alias("genre")
        ).groupBy("genre").count().orderBy(desc("count"))
    
    def get_director_productivity(self):
        """Analyze most productive directors"""
        # Using the crew_df which has directors column
        return self.crew_df.select(
            explode(split("directors", ",")).alias("director_id")
        ).filter(
            col("director_id").isNotNull()
        ).groupBy("director_id").count().orderBy(desc("count")).limit(10)
    
    def get_job_distribution(self):
        """Analyze distribution of roles in film crew"""
        # Using principals_df instead of crew_df
        return self.principals_df.groupBy("category", "job").count().orderBy(desc("count"))
    
    def get_multi_role_people(self):
        """Find people with multiple roles"""
        return self.principals_df.groupBy("nconst").agg(
            count("category").alias("num_roles"),
            collect_set("category").alias("roles")
        ).orderBy(desc("num_roles")).limit(10)
    
    def get_collaboration_network(self):
        """Analyze collaboration network between directors and producers"""
        # Using principals_df for this analysis
        directors = self.principals_df.filter(
            col("category") == "director"
        ).select("tconst", "nconst").withColumnRenamed("nconst", "director_id")
        
        producers = self.principals_df.filter(
            col("category") == "producer"
        ).select("tconst", "nconst").withColumnRenamed("nconst", "producer_id")
        
        return directors.join(producers, "tconst").groupBy(
            "director_id", "producer_id"
        ).count().orderBy(desc("count"))