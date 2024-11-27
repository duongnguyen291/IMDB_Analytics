import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, col, count, desc, avg, 
    sum , when, concat_ws,
    split, year, struct, dense_rank,
    var_pop,  
)
from pyspark.sql.types import DoubleType

class AdvancedAnalysis:
    def __init__(self, spark_session):
        """
        Initialize AdvancedAnalysis with a SparkSession
        
        :param spark_session: Active Spark session
        """
        self.spark = spark_session
    
    def compute_genre_rating_correlation(self, title_basics_df, title_ratings_df):
        """
        Compute correlation between genre and average rating
        
        :param title_basics_df: DataFrame with title basics
        :param title_ratings_df: DataFrame with title ratings
        :return: Correlation matrix of genres with ratings
        """
        # One-hot encode genres
        genre_encoded_df = (
            title_basics_df
            .join(title_ratings_df, 'tconst')
            .select(
                F.explode(F.split('genres', ',')).alias('genre'), 
                'averageRating'
            )
            .groupBy('genre')
            .agg(
                F.avg('averageRating').alias('avg_rating'),
                F.count('*').alias('genre_count')
            )
            .filter(F.col('genre') != '\\N')
        )
        
        # Prepare data for correlation
        assembler = VectorAssembler(
            inputCols=['avg_rating', 'genre_count'], 
            outputCol='features'
        )
        features_df = assembler.transform(genre_encoded_df)
        
        # Compute correlation matrix
        correlation_matrix = Correlation.corr(features_df, 'features')
        return correlation_matrix
    
    def analyze_crew_contribution(self, title_basics_df, title_crew_df, title_ratings_df, top_n=10):
        """
        Analyze top directors and writers based on their titles' ratings
        
        :param title_basics_df: DataFrame with title basics
        :param title_crew_df: DataFrame with crew information
        :param title_ratings_df: DataFrame with title ratings
        :param top_n: Number of top crew members to return
        :return: DataFrame of top crew members by average title rating
        """
        # Explode directors and join with basics and ratings
        directors_performance = (
            title_crew_df
            .select(F.explode(F.split('directors', ',')).alias('nconst'), 'tconst')
            .join(title_basics_df, 'tconst')
            .join(title_ratings_df, 'tconst')
            .groupBy('nconst')
            .agg(
                F.avg('averageRating').alias('avg_title_rating'),
                F.count('*').alias('total_titles'),
                F.collect_list('primaryTitle').alias('titles')
            )
            .filter(F.col('total_titles') > 3)  # Minimum 3 titles to be considered
            .orderBy(F.desc('avg_title_rating'))
            .limit(top_n)
        )
        return directors_performance
    
    def compute_rating_complexity_analysis(self, title_basics_df, title_ratings_df):
        """
        Analyze rating complexity across different title types
        
        :param title_basics_df: DataFrame with title basics
        :param title_ratings_df: DataFrame with title ratings
        :return: DataFrame with rating statistics by title type
        """
        rating_complexity = (
            title_basics_df
            .join(title_ratings_df, 'tconst')
            .groupBy('titleType')
            .agg(
                F.avg('averageRating').alias('mean_rating'),
                F.stddev('averageRating').alias('rating_variance'),
                F.min('averageRating').alias('min_rating'),
                F.max('averageRating').alias('max_rating'),
                F.percentile_approx('averageRating', [0.25, 0.5, 0.75]).alias('rating_quartiles')
            )
        )
        return rating_complexity
    
    def analyze_runtime_rating_relationship(self, title_basics_df, title_ratings_df):
        """
        Explore relationship between runtime and ratings
        
        :param title_basics_df: DataFrame with title basics
        :param title_ratings_df: DataFrame with title ratings
        :return: DataFrame showing runtime-rating correlation
        """
        runtime_rating_analysis = (
            title_basics_df
            .join(title_ratings_df, 'tconst')
            .filter(
                (F.col('runtimeMinutes') != '\\N') & 
                (F.col('runtimeMinutes').cast('int').isNotNull())
            )
            .select(
                'runtimeMinutes', 
                'averageRating', 
                'titleType'
            )
        )
        
        # Compute correlation
        assembler = VectorAssembler(
            inputCols=['runtimeMinutes', 'averageRating'], 
            outputCol='features'
        )
        features_df = assembler.transform(runtime_rating_analysis)
        correlation_matrix = Correlation.corr(features_df, 'features')
        
        return {
            'runtime_rating_df': runtime_rating_analysis,
            'correlation_matrix': correlation_matrix
        }
#bắt đầu ở đây
    def analyze_comprehensive_genre_popularity(
        titles_df, 
        ratings_df, 
        crew_path=None, 
        principals_path=None
    ):
        """
        Comprehensive genre popularity analysis using multiple IMDb datasets
    
        :param titles_path: Path to title.basics.tsv.gz
        :param ratings_path: Path to title.ratings.tsv.gz
        :param crew_path: Optional path to title.crew.tsv.gz
        :param principals_path: Optional path to title.principals.tsv.gz
        :return: DataFrame with comprehensive genre popularity metrics
        """
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Comprehensive Genre Popularity Analysis") \
            .getOrCreate()
    
        # Read input files
    
        # Filter out adult and low-relevance titles
        filtered_titles = titles_df.filter(
            (col('isAdult') == '0') & 
            (col('genres') != '\\N') &
            (col('titleType').isin(['movie', 'tvseries']))
        )
    
    # Convert ratings to numeric, handling potential null values
        ratings_df = ratings_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )
    
    # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            ratings_df, 
            filtered_titles['tconst'] == ratings_df['tconst'], 
            'left'
        )
    
    # Optional: Join with crew data for additional insights
        if crew_path:
            crew_df = spark.read.csv(
                crew_path, 
                sep='\t', 
                header=True, 
                nullValue='\\N'
            )
            titles_with_ratings = titles_with_ratings.join(
                crew_df, 
                titles_with_ratings['tconst'] == crew_df['tconst'], 
            'left'
            )
    
    # Explode genres and aggregate
        genre_popularity = titles_with_ratings \
            .select(
                explode(col('genres').split(',')).alias('genre'),
                col('averageRating').alias('rating'),
                col('numVotes').alias('votes'),
                col('titleType')
            ) \
            .groupBy('genre') \
            .agg(
            # Basic metrics
                count('*').alias('total_titles'),
                avg('rating').alias('avg_rating'),
                sum('votes').alias('total_votes'),
            
            # Weighted popularity score
            # Combine rating and vote count for a comprehensive metric
                (avg('rating') * sum('votes')).alias('weighted_popularity'),
            
            # Breakdown by title type
                count(when(col('titleType') == 'movie', 1)).alias('movie_count'),
                count(when(col('titleType') == 'tvseries', 1)).alias('tvseries_count')
            )
    
    # Calculate percentages and final popularity score
        total_titles = filtered_titles.count()
    
        final_genre_popularity = genre_popularity \
            .withColumn('title_percentage', 
                    col('total_titles') / total_titles * 100) \
            .withColumn('popularity_score', 
                    col('weighted_popularity') / sum('weighted_popularity').over()) \
            .orderBy(desc('popularity_score'))
    
        return final_genre_popularity

    def analyze_genre_trends(basics_path, ratings_path):
        spark = SparkSession.builder \
        .appName("GenreTrendsAnalysis") \
        .getOrCreate()
    
    # Read title basics and ratings data
        title_basics = spark.read.csv(basics_path, sep='\t', header=True, nullValue='\\N')
        title_ratings = spark.read.csv(ratings_path, sep='\t', header=True, nullValue='\\N')
    
    # Prepare the basics dataframe
        basics_prepared = title_basics.select(
            col("tconst"),
            col("startYear"),
            explode(split(col("genres"), ",")).alias("genre")
        ).filter(col("startYear") != "\\N")  # Remove entries without a year
    
    # Cast startYear to integer
        basics_prepared = basics_prepared.withColumn(
            "startYear", 
            col("startYear").cast("integer")
        )
    
    # Join with ratings to get rating information
        genre_trends = basics_prepared.join(
            title_ratings, 
            "tconst"
        ).select(
            "startYear",
            "genre",
            "averageRating",
            "numVotes"
        )
    
        # Compute genre trends per year
        genre_year_stats = genre_trends.groupBy(
            "startYear", 
            "genre"
        ).agg(
            avg("averageRating").alias("avg_rating"),
            count("*").alias("title_count"),
            sum("numVotes").alias("total_votes")
        )
        
        # Filter out very sparse genres (optional)
        genre_popularity = genre_year_stats.filter(col("title_count") > 10)
        
        # Find top genres by total votes for each year
        window_spec = Window.partitionBy("startYear").orderBy(desc("total_votes"))
        top_genres_by_year = genre_popularity.withColumn(
            "genre_rank", 
            dense_rank().over(window_spec)
        ).filter(col("genre_rank") <= 5)
        
        # Action: Show top genres trend
        top_genres_by_year.orderBy("startYear", "genre_rank").show(100, truncate=False)
        
        # Optional: Save results to a CSV
        top_genres_by_year.write.mode("overwrite").csv(
            "genre_trends_analysis", 
            header=True
        )
        
        return top_genres_by_year
    def analyze_genre_popularity(spark, titles_path, akas_path, ratings_path):
        """
        Analyze genre popularity by region and language
        
        :param spark: SparkSession
        :param titles_path: Path to title.basics.tsv.gz
        :param akas_path: Path to title.akas.tsv.gz
        :param ratings_path: Path to title.ratings.tsv.gz
        :return: DataFrame with genre popularity metrics
        """
        # Load datasets
        titles_df = spark.read.option("sep", "\t").option("header", "true").option("nullValue", "\\N").csv(titles_path)
        akas_df = spark.read.option("sep", "\t").option("header", "true").option("nullValue", "\\N").csv(akas_path)
        ratings_df = spark.read.option("sep", "\t").option("header", "true").option("nullValue", "\\N").csv(ratings_path)
        
        # Prepare and join data
        genre_analysis = (titles_df
            .filter((col("titleType").isin(["movie", "tvSeries"])) & (col("isAdult") == 0))
            .select("tconst", explode(split(col("genres"), ",")).alias("genre"))
            .join(ratings_df, "tconst")
            .join(akas_df, "tconst")
            .select("tconst", "genre", "averageRating", "numVotes", "region", "language")
        )
        
        # Analyze genre popularity by region
        genre_popularity_by_region = (genre_analysis
            .groupBy("region", "genre")
            .agg(
                count("tconst").alias("total_titles"),
                avg("averageRating").alias("avg_rating"),
                avg("numVotes").alias("avg_votes")
            )
            .orderBy(desc("total_titles"))
        )
        
        # Analyze genre popularity by language
        genre_popularity_by_language = (genre_analysis
            .groupBy("language", "genre")
            .agg(
                count("tconst").alias("total_titles"),
                avg("averageRating").alias("avg_rating"),
                avg("numVotes").alias("avg_votes")
            )
            .orderBy(desc("total_titles"))
        )
        
        return {
            "by_region": genre_popularity_by_region,
            "by_language": genre_popularity_by_language
        }
    def analyze_emerging_declining_genres(
        titles_df, 
       ratings_df, 
        crew_path=None, 
    ):
        """
        Analyze emerging or declining genres based on popularity trends over time using IMDb datasets.

        :param titles_path: Path to title.basics.tsv.gz
        :param ratings_path: Path to title.ratings.tsv.gz
        :param crew_path: Optional path to title.crew.tsv.gz
        :param principals_path: Optional path to title.principals.tsv.gz
        :return: DataFrame with genre popularity trends over time
        """
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Emerging or Declining Genres Analysis") \
            .getOrCreate()

        # Filter out adult and low-relevance titles
        filtered_titles = titles_df.filter(
            (col('isAdult') == '0') & 
            (col('genres') != '\\N') & 
            (col('titleType').isin(['movie', 'tvseries']))
        )

        # Convert ratings to numeric, handling potential null values
        ratings_df = ratings_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )

        # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            ratings_df, 
            filtered_titles['tconst'] == ratings_df['tconst'], 
            'left'
        )

        # Optional: Join with crew data for additional insights
        if crew_path:
            crew_df = spark.read.csv(
                crew_path, 
                sep='\t', 
                header=True, 
                nullValue='\\N'
            )
            titles_with_ratings = titles_with_ratings.join(
                crew_df, 
                titles_with_ratings['tconst'] == crew_df['tconst'], 
                'left'
            )

        # Explode genres and aggregate to get genre-wise popularity metrics
        genre_popularity = titles_with_ratings \
            .select(
                explode(split(col('genres'), ',')).alias('genre'),
                col('averageRating').alias('rating'),
                col('numVotes').alias('votes'),
                col('titleType'),
                col('startYear')  # Adding startYear for time-based analysis
            ) \
            .filter(col('startYear').isNotNull())  # Remove entries without a start year

        # Group by genre and startYear to track trends over time
        genre_trends = genre_popularity \
            .groupBy('genre', 'startYear') \
            .agg(
                count('*').alias('total_titles'),
                avg('rating').alias('avg_rating'),
                avg('votes').alias('avg_votes'),
                sum('votes').alias('total_votes')
            )

        # Calculate weighted popularity score
        genre_trends = genre_trends.withColumn(
            'weighted_popularity',
            col('avg_rating') * col('total_votes')
        )

        # Calculate percentage of total titles per year
        total_titles_per_year = genre_trends.groupBy('startYear').agg(
            sum('total_titles').alias('total_titles_year')
        )

        genre_trends = genre_trends.join(
            total_titles_per_year,
            genre_trends['startYear'] == total_titles_per_year['startYear'],
            'left'
        ).withColumn(
            'title_percentage',
            col('total_titles') / col('total_titles_year') * 100
        )

        # Calculate trends: change in genre popularity over time
        window_spec = Window.partitionBy('genre').orderBy('startYear')

        genre_trends = genre_trends.withColumn(
            'popularity_change',
            col('weighted_popularity') - lag('weighted_popularity', 1).over(window_spec)
        )

        # Identify emerging genres (positive trend) and declining genres (negative trend)
        emerging_genres = genre_trends.filter(col('popularity_change') > 0)
        declining_genres = genre_trends.filter(col('popularity_change') < 0)

        # Combine the results into one DataFrame for better insights
        emerging_declining_genres = emerging_genres.unionByName(declining_genres)

        # Order by popularity change (ascending for declining, descending for emerging)
        final_result = emerging_declining_genres \
            .orderBy(
                col('popularity_change').desc(),  # Top emerging genres first
                col('popularity_change').asc()   # Declining genres after
            )

        return final_result
    def analyze_top_rated_titles(
        titles_df, 
        ratings_df, 
        crew_path=None, 
        principals_path=None
    ):
        """
        Analyze top-rated titles across different categories (genres, title types) using IMDb datasets.

        :param titles_path: Path to title.basics.tsv.gz
        :param ratings_path: Path to title.ratings.tsv.gz
        :param crew_path: Optional path to title.crew.tsv.gz
        :param principals_path: Optional path to title.principals.tsv.gz
        :return: DataFrame with top-rated titles by genre and title type
        """
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Top-Rated Titles Analysis") \
            .getOrCreate()


        # Filter out adult content and invalid titles
        filtered_titles = titles_df.filter(
            (col('isAdult') == '0') & 
            (col('genres') != '\\N') & 
            (col('titleType').isin(['movie', 'tvseries']))
        )

        # Convert ratings to numeric types
        ratings_df = ratings_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )

        # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            ratings_df, 
            filtered_titles['tconst'] == ratings_df['tconst'], 
            'left'
        )

        # Optional: Join with crew data for additional insights
        if crew_path:
            crew_df = spark.read.csv(
                crew_path, 
                sep='\t', 
                header=True, 
                nullValue='\\N'
            )
            titles_with_ratings = titles_with_ratings.join(
                crew_df, 
                titles_with_ratings['tconst'] == crew_df['tconst'], 
                'left'
            )

        # Explode genres and aggregate to get genre-wise popularity metrics
        genre_ratings = titles_with_ratings \
            .select(
                explode(split(col('genres'), ',')).alias('genre'),
                col('averageRating').alias('rating'),
                col('titleType')
            )

        # Get top-rated titles per genre
        top_rated_genre = genre_ratings \
            .groupBy('genre') \
            .agg(
                avg('rating').alias('avg_rating'),
                count('*').alias('num_titles')
            ) \
            .orderBy(desc('avg_rating'))  # Top-rated genre first

        # Get top-rated titles by title type (e.g., movies vs tvseries)
        top_rated_title_type = titles_with_ratings \
            .groupBy('titleType') \
            .agg(
                avg('averageRating').alias('avg_rating'),
                count('*').alias('num_titles')
            ) \
            .orderBy(desc('avg_rating'))  # Top-rated title type first

        # Get top-rated titles overall
        top_rated_overall = titles_with_ratings \
            .select('tconst', 'primaryTitle', 'averageRating', 'titleType') \
            .orderBy(desc('averageRating')) \
            .limit(10)  # You can adjust the number of top titles

        # Combine all results into one DataFrame for a comprehensive overview
        top_rated = {
            "top_rated_genre": top_rated_genre,
            "top_rated_title_type": top_rated_title_type,
            "top_rated_overall": top_rated_overall
        }

        return top_rated
    def analyze_ratings_by_title_type(
        titles_df, 
        ratings_df, 
        crew_path=None, 
        principals_path=None
    ):
        """
        Analyze the correlation between ratings and title type (movie, tvseries) using IMDb datasets.

        :param titles_path: Path to title.basics.tsv.gz
        :param ratings_path: Path to title.ratings.tsv.gz
        :param crew_path: Optional path to title.crew.tsv.gz
        :param principals_path: Optional path to title.principals.tsv.gz
        :return: DataFrame with average ratings per title type and visualizations
        """
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Correlation Between Ratings and Title Type") \
            .getOrCreate()

        # Filter out adult content and invalid titles
        filtered_titles = titles_df.filter(
            (col('isAdult') == '0') & 
            (col('genres') != '\\N') & 
            (col('titleType').isin(['movie', 'tvseries']))
        )

        # Convert ratings to numeric types
        ratings_df = ratings_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )

        # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            ratings_df, 
            filtered_titles['tconst'] == ratings_df['tconst'], 
            'left'
        )

        # 1. Calculate the average rating by title type (movie, tvseries)
        avg_rating_by_title_type = titles_with_ratings \
            .groupBy('titleType') \
            .agg(
                avg('averageRating').alias('avg_rating'),
                count('*').alias('num_titles')
            ) \
            .orderBy(desc('avg_rating'))

        # Optional: If you want to perform a t-test or further statistical analysis,
        # you could extract the ratings for each title type and compare them.

        return avg_rating_by_title_type
    def analyze_director_writer_performance(
        titles_df, 
        ratings_df, 
        crew_df, 
        principals_df
    ):
        """
        Analyze the performance metrics of directors and writers based on IMDb datasets.
        Metrics include average rating, total votes, number of titles, and weighted popularity.

        :param titles_path: Path to title.basics.tsv.gz
        :param ratings_path: Path to title.ratings.tsv.gz
        :param crew_path: Path to title.crew.tsv.gz
        :param principals_path: Path to title.principals.tsv.gz
        :return: DataFrame with performance metrics for directors and writers
        """
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Director and Writer Performance Analysis") \
            .getOrCreate()


        # Filter out adult content and invalid titles
        filtered_titles = titles_df.filter(
            (col('isAdult') == '0') & 
            (col('genres') != '\\N') & 
            (col('titleType').isin(['movie', 'tvseries']))
        )

        # Convert ratings to numeric types
        ratings_df = ratings_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )

        # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            ratings_df, 
            filtered_titles['tconst'] == ratings_df['tconst'], 
            'left'
        )

        # Director Performance Metrics
        directors_df = crew_df.filter(col('job') == 'Director') \
            .select('tconst', 'nconst').distinct()  # Only distinct directors for each title

        # Join director data with title ratings
        directors_with_ratings = titles_with_ratings.join(
            directors_df, 
            titles_with_ratings['tconst'] == directors_df['tconst'],
            'inner'
        )

        # Aggregate metrics for directors
        director_performance = directors_with_ratings \
            .groupBy('nconst') \
            .agg(
                avg('averageRating').alias('avg_rating'),
                count('tconst').alias('num_titles'),
                sum('numVotes').alias('total_votes'),
                (avg('averageRating') * sum('numVotes')).alias('weighted_popularity')
            ) \
            .orderBy(desc('weighted_popularity'))  # Sort by weighted popularity

        # Writer Performance Metrics
        writers_df = principals_df.filter(col('category') == 'writer') \
            .select('tconst', 'nconst').distinct()  # Only distinct writers for each title

        # Join writer data with title ratings
        writers_with_ratings = titles_with_ratings.join(
            writers_df, 
            titles_with_ratings['tconst'] == writers_df['tconst'],
            'inner'
        )

        # Aggregate metrics for writers
        writer_performance = writers_with_ratings \
            .groupBy('nconst') \
            .agg(
                avg('averageRating').alias('avg_rating'),
                count('tconst').alias('num_titles'),
                sum('numVotes').alias('total_votes'),
                (avg('averageRating') * sum('numVotes')).alias('weighted_popularity')
            ) \
            .orderBy(desc('weighted_popularity'))  # Sort by weighted popularity

        # Combine director and writer performance data
        combined_performance = director_performance.unionByName(writer_performance)

        return combined_performance
    def analyze_crew_performance_and_ratings(
        titles_df, 
        ratings_df, 
        crew_df, 
        principals_df
    ):
        """
        Analyze the correlation between crew members (e.g., directors, writers) and title ratings.
        The metrics include average rating, number of titles, total votes, and weighted popularity.

        :param titles_df: Path to title.basics.tsv.gz
        :param ratings_df: Path to title.ratings.tsv.gz
        :param crew_df: Path to title.crew.tsv.gz
        :param principals_df: Path to title.principals.tsv.gz
        :return: DataFrame with performance metrics for crew members (directors, writers) and their correlation with ratings
        """
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Crew Performance and Ratings Analysis") \
            .getOrCreate()

        # Filter out adult content and invalid titles
        filtered_titles = titles_df.filter(
            (col('isAdult') == '0') & 
            (col('genres') != '\\N') & 
            (col('titleType').isin(['movie', 'tvseries']))
        )

        # Convert ratings to numeric types
        ratings_df = ratings_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )

        # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            ratings_df, 
            filtered_titles['tconst'] == ratings_df['tconst'], 
            'left'
        )

        # Director Performance Metrics (from crew_df)
        directors_df = crew_df.filter(col('job') == 'Director') \
            .select('tconst', 'nconst').distinct()  # Only distinct directors for each title

        # Join director data with title ratings
        directors_with_ratings = titles_with_ratings.join(
            directors_df, 
            titles_with_ratings['tconst'] == directors_df['tconst'],
            'inner'
        )

        # Aggregate metrics for directors
        director_performance = directors_with_ratings \
            .groupBy('nconst') \
            .agg(
                avg('averageRating').alias('avg_rating'),
                count('tconst').alias('num_titles'),
                sum('numVotes').alias('total_votes'),
                (avg('averageRating') * sum('numVotes')).alias('weighted_popularity')
            ) \
            .orderBy(desc('weighted_popularity'))  # Sort by weighted popularity

        # Writer Performance Metrics (from principals_df)
        writers_df = principals_df.filter(col('category') == 'writer') \
            .select('tconst', 'nconst').distinct()  # Only distinct writers for each title

        # Join writer data with title ratings
        writers_with_ratings = titles_with_ratings.join(
            writers_df, 
            titles_with_ratings['tconst'] == writers_df['tconst'],
            'inner'
        )

        # Aggregate metrics for writers
        writer_performance = writers_with_ratings \
            .groupBy('nconst') \
            .agg(
                avg('averageRating').alias('avg_rating'),
                count('tconst').alias('num_titles'),
                sum('numVotes').alias('total_votes'),
                (avg('averageRating') * sum('numVotes')).alias('weighted_popularity')
            ) \
            .orderBy(desc('weighted_popularity'))  # Sort by weighted popularity

        # Combine director and writer performance data
        combined_performance = director_performance.unionByName(writer_performance)

        # Optionally: Analyze correlation or perform statistical tests here if needed
        # This could include looking at correlations between weighted popularity and ratings, etc.

        return combined_performance
    def identify_consistently_high_performing_crew(
        titles_df, 
        ratings_df, 
        crew_df, 
        principals_df,
        avg_rating_threshold=8.0,  # Threshold for "high-performing" average rating
        min_titles_threshold=5     # Minimum number of titles to consider consistency
    ):
        """
        Identify consistently high-performing directors and writers based on IMDb ratings and votes.
        
        :param titles_path: Path to title.basics.tsv.gz
        :param ratings_path: Path to title.ratings.tsv.gz
        :param crew_path: Path to title.crew.tsv.gz
        :param principals_path: Path to title.principals.tsv.gz
        :param avg_rating_threshold: The threshold for high-performing average ratings (default is 8.0)
        :param min_titles_threshold: Minimum number of titles to be considered as consistently high-performing (default is 5)
        :return: DataFrame with consistently high-performing directors and writers
        """
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Consistently High-Performing Crew Analysis") \
            .getOrCreate()


        # Filter out adult content and invalid titles
        filtered_titles = titles_df.filter(
            (col('isAdult') == '0') & 
            (col('genres') != '\\N') & 
            (col('titleType').isin(['movie', 'tvseries']))
        )

        # Convert ratings to numeric types
        ratings_df = ratings_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )

        # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            ratings_df, 
            filtered_titles['tconst'] == ratings_df['tconst'], 
            'left'
        )

        # Director Performance Metrics (from crew_df)
        directors_df = crew_df.filter(col('job') == 'Director') \
            .select('tconst', 'nconst').distinct()  # Only distinct directors for each title

        # Join director data with title ratings
        directors_with_ratings = titles_with_ratings.join(
            directors_df, 
            titles_with_ratings['tconst'] == directors_df['tconst'],
            'inner'
        )

        # Aggregate metrics for directors
        director_performance = directors_with_ratings \
            .groupBy('nconst') \
            .agg(
                avg('averageRating').alias('avg_rating'),
                count('tconst').alias('num_titles'),
                sum('numVotes').alias('total_votes'),
                (avg('averageRating') * sum('numVotes')).alias('weighted_popularity')
            ) \
            .filter(
                (col('avg_rating') >= avg_rating_threshold) &  # Apply average rating threshold
                (col('num_titles') >= min_titles_threshold)   # Apply minimum number of titles threshold
            ) \
            .orderBy(desc('avg_rating'))  # Sort by avg rating

        # Writer Performance Metrics (from principals_df)
        writers_df = principals_df.filter(col('category') == 'writer') \
            .select('tconst', 'nconst').distinct()  # Only distinct writers for each title

        # Join writer data with title ratings
        writers_with_ratings = titles_with_ratings.join(
            writers_df, 
            titles_with_ratings['tconst'] == writers_df['tconst'],
            'inner'
        )

        # Aggregate metrics for writers
        writer_performance = writers_with_ratings \
            .groupBy('nconst') \
            .agg(
                avg('averageRating').alias('avg_rating'),
                count('tconst').alias('num_titles'),
                sum('numVotes').alias('total_votes'),
                (avg('averageRating') * sum('numVotes')).alias('weighted_popularity')
            ) \
            .filter(
                (col('avg_rating') >= avg_rating_threshold) &  # Apply average rating threshold
                (col('num_titles') >= min_titles_threshold)   # Apply minimum number of titles threshold
            ) \
            .orderBy(desc('avg_rating'))  # Sort by avg rating

        # Combine director and writer performance data
        combined_performance = director_performance.unionByName(writer_performance)

        return combined_performance
    def analyze_rating_variance_by_title_type(
        titles_df, 
        ratings_df
    ):
        """
        Analyze the variance in ratings across different title types using IMDb datasets.

        :param titles_path: Path to title.basics.tsv.gz
        :param ratings_path: Path to title.ratings.tsv.gz
        :return: DataFrame with rating variance, average rating, and title count by title type
        """
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Rating Variance by Title Type") \
            .getOrCreate()

        # Filter out adult content and invalid titles
        filtered_titles = titles_df.filter(
            (col('isAdult') == '0') & 
            (col('titleType').isNotNull())
        )

        # Convert ratings to numeric types
        ratings_df = ratings_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )

        # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            ratings_df, 
            filtered_titles['tconst'] == ratings_df['tconst'], 
            'inner'
        )

        # Calculate variance, average rating, and count by title type
        rating_variance_by_type = titles_with_ratings \
            .groupBy('titleType') \
            .agg(
                var_pop('averageRating').alias('rating_variance'),
                avg('averageRating').alias('avg_rating'),
                count('tconst').alias('num_titles')
            ) \
            .orderBy(desc('rating_variance'))  # Sort by variance descending

        return rating_variance_by_type
