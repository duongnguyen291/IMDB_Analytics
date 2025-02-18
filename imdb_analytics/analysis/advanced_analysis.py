import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, col, count, desc, avg, 
    sum , when, concat_ws, asc,
    split, year, struct, dense_rank,
    var_pop,  stddev
)
from pyspark.sql.types import DoubleType 
import matplotlib.pyplot as plt
import seaborn as sns
import time


class AdvancedAnalysis:
    def __init__(self, spark_session):
        """
        Initialize AdvancedAnalysis with a SparkSession
        
        :param spark_session: Active Spark session
        """
        self.spark = spark_session
    
    def compute_genre_rating_correlation(self, title_basics_df, title_rating_df):
        """
        Compute correlation between genre and average rating
        
        :param title_basics_df: DataFrame with title basics
        :param title_rating_df: DataFrame with title ratings
        :return: Correlation matrix of genres with ratings
        """
        # One-hot encode genres
        genre_encoded_df = (
            title_basics_df
            .join(title_rating_df, 'tconst')
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
    
    def analyze_crew_contribution(self, title_basics_df, title_crew_df, title_rating_df, top_n=10):
        """
        Analyze top directors and writers based on their titles' ratings
        
        :param title_basics_df: DataFrame with title basics
        :param title_crew_df: DataFrame with crew information
        :param title_rating_df: DataFrame with title ratings
        :param top_n: Number of top crew members to return
        :return: DataFrame of top crew members by average title rating
        """
        # Explode directors and join with basics and ratings
        directors_performance = (
            title_crew_df
            .select(F.explode(F.split('directors', ',')).alias('nconst'), 'tconst')
            .join(title_basics_df, 'tconst')
            .join(title_rating_df, 'tconst')
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
    
    def analyze_runtime_rating_relationship(self, title_basics_df, title_rating_df):
        """
        Explore relationship between runtime and ratings
        
        :param title_basics_df: DataFrame with title basics
        :param title_rating_df: DataFrame with title ratings
        :return: DataFrame showing runtime-rating correlation
        """
        runtime_rating_analysis = (
            title_basics_df
            .join(title_rating_df, 'tconst')
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
        rating_df, 
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
        rating_df = rating_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )
    
    # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            rating_df, 
            filtered_titles['tconst'] == rating_df['tconst'], 
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
        rating_df = spark.read.option("sep", "\t").option("header", "true").option("nullValue", "\\N").csv(ratings_path)
        
        # Prepare and join data
        genre_analysis = (titles_df
            .filter((col("titleType").isin(["movie", "tvSeries"])) & (col("isAdult") == 0))
            .select("tconst", explode(split(col("genres"), ",")).alias("genre"))
            .join(rating_df, "tconst")
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
       rating_df, 
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
        rating_df = rating_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )

        # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            rating_df, 
            filtered_titles['tconst'] == rating_df['tconst'], 
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
        rating_df, 
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
        rating_df = rating_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )

        # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            rating_df, 
            filtered_titles['tconst'] == rating_df['tconst'], 
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
    def analyze_rating_distribution(
        titles_path, 
        ratings_path, 
        crew_path=None, 
        principals_path=None
    ):
        """
        Analyze the distribution of ratings across IMDb titles.

        :param titles_path: Path to title.basics.tsv.gz
        :param ratings_path: Path to title.ratings.tsv.gz
        :param crew_path: Optional path to title.crew.tsv.gz
        :param principals_path: Optional path to title.principals.tsv.gz
        :return: DataFrame with rating distribution metrics
        """
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Rating Distribution Analysis") \
            .getOrCreate()

        # Read input files
        titles_df = spark.read.csv(
            titles_path, 
            sep='\t', 
            header=True, 
            nullValue='\\N'
        )

        rating_df = spark.read.csv(
            ratings_path, 
            sep='\t', 
            header=True, 
            nullValue='\\N'
        )

        # Filter out adult content and invalid titles
        filtered_titles = titles_df.filter(
            (col('isAdult') == '0') & 
            (col('genres') != '\\N') & 
            (col('titleType').isin(['movie', 'tvseries']))
        )

        # Convert ratings to numeric types
        rating_df = rating_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )

        # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            rating_df, 
            filtered_titles['tconst'] == rating_df['tconst'], 
            'left'
        )

        # 1. Overall Distribution of Ratings
        rating_distribution = titles_with_ratings \
            .groupBy('averageRating') \
            .agg(
                count('*').alias('num_titles')
            ) \
            .orderBy(desc('averageRating'))  # Sort by rating value

        # 2. Rating Ranges: Create categories like 0-2, 2-4, 4-6, etc.
        rating_ranges = titles_with_ratings \
            .withColumn('rating_range', 
                        when(col('averageRating') < 2, '0-2')
                        .when(col('averageRating') < 4, '2-4')
                        .when(col('averageRating') < 6, '4-6')
                        .when(col('averageRating') < 8, '6-8')
                        .otherwise('8-10')
            ) \
            .groupBy('rating_range') \
            .agg(
                count('*').alias('num_titles')
            ) \
            .orderBy('rating_range')

        # 3. Plotting the Distribution using a Histogram
        ratings_data = titles_with_ratings.select('averageRating').rdd.flatMap(lambda x: x).collect()
        
        # Plot the distribution
        plt.figure(figsize=(10,6))
        plt.hist(ratings_data, bins=20, edgecolor='black', color='skyblue')
        plt.title("Distribution of IMDb Ratings")
        plt.xlabel('Rating')
        plt.ylabel('Number of Titles')
        plt.grid(True)
        plt.show()

        return {
            "rating_distribution": rating_distribution,
            "rating_ranges": rating_ranges
        }
    
    def analyze_ratings_by_title_type(
        titles_df, 
        rating_df, 
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
        rating_df = rating_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )

        # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            rating_df, 
            filtered_titles['tconst'] == rating_df['tconst'], 
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
         # 2. Visualize ratings by title type (boxplot or histogram)
        title_type_data = titles_with_ratings.select('averageRating', 'titleType').toPandas()

        # Set up the plot
        plt.figure(figsize=(10,6))
        sns.boxplot(data=title_type_data, x='titleType', y='averageRating', palette="Set2")
        plt.title('Distribution of Ratings by Title Type')
        plt.xlabel('Title Type')
        plt.ylabel('Average Rating')
        plt.show()
        # Optional: If you want to perform a t-test or further statistical analysis,
        # you could extract the ratings for each title type and compare them.

        return avg_rating_by_title_type
    def analyze_director_writer_performance(
        titles_df, 
        rating_df, 
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
        rating_df = rating_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )

        # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            rating_df, 
            filtered_titles['tconst'] == rating_df['tconst'], 
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
        rating_df, 
        crew_df, 
        principals_df
    ):
        """
        Analyze the correlation between crew members (e.g., directors, writers) and title ratings.
        The metrics include average rating, number of titles, total votes, and weighted popularity.

        :param titles_df: Path to title.basics.tsv.gz
        :param rating_df: Path to title.ratings.tsv.gz
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
        rating_df = rating_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )

        # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            rating_df, 
            filtered_titles['tconst'] == rating_df['tconst'], 
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
        rating_df, 
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
        rating_df = rating_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )

        # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            rating_df, 
            filtered_titles['tconst'] == rating_df['tconst'], 
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
        rating_df
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
        rating_df = rating_df.withColumn(
            'averageRating', 
            col('averageRating').cast(DoubleType())
        ).withColumn(
            'numVotes', 
            col('numVotes').cast('integer')
        )

        # Join titles with ratings
        titles_with_ratings = filtered_titles.join(
            rating_df, 
            filtered_titles['tconst'] == rating_df['tconst'], 
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
    def analyze_rating_consistency(
        titles_df, 
        rating_df, 
        crew_df, 
        output_crew_insights=False
    ):
        """
        Analyze rating consistency across IMDb titles.

        :param titles_path: Path to title.basics.tsv.gz
        :param ratings_path: Path to title.ratings.tsv.gz
        :param crew_path: Path to title.crew.tsv.gz
        :param output_crew_insights: If True, includes crew-specific insights
        :return: Dictionary containing dataframes for rating consistency analysis
        """
       

        spark = SparkSession.builder \
            .appName("Rating Consistency Analysis") \
            .getOrCreate()

        # Filter and preprocess titles
        titles_df = titles_df.filter((col('isAdult') == '0') & (col('genres') != '\\N'))
        titles_df = titles_df.withColumn('genres', split(col('genres'), ','))  # Split genres into array
        rating_df = rating_df.withColumn('averageRating', col('averageRating').cast(DoubleType()))
        rating_df = rating_df.withColumn('numVotes', col('numVotes').cast('integer'))

        # Join datasets
        titles_with_ratings = titles_df.join(rating_df, 'tconst', 'left')
        titles_with_crew = titles_with_ratings.join(crew_df, 'tconst', 'left')

        # Explode genres for analysis
        titles_with_genres = titles_with_ratings.withColumn('genre', explode(col('genres')))

        # 1. Consistency by Genre
        genre_consistency = titles_with_genres.groupBy('genre') \
            .agg(
                avg('averageRating').alias('avg_rating'),
                stddev('averageRating').alias('rating_stddev'),
                count('*').alias('num_titles')
            ) \
            .orderBy(desc('avg_rating'))

        # 2. Consistency by Directors and Writers
        crew_consistency = None
        if output_crew_insights:
            directors_consistency = titles_with_crew.withColumn('director', explode(split(col('directors'), ','))) \
                .groupBy('director') \
                .agg(
                    avg('averageRating').alias('avg_rating'),
                    stddev('averageRating').alias('rating_stddev'),
                    count('*').alias('num_titles')
                ) \
                .orderBy(desc('avg_rating'))

            writers_consistency = titles_with_crew.withColumn('writer', explode(split(col('writers'), ','))) \
                .groupBy('writer') \
                .agg(
                    avg('averageRating').alias('avg_rating'),
                    stddev('averageRating').alias('rating_stddev'),
                    count('*').alias('num_titles')
                ) \
                .orderBy(desc('avg_rating'))

            crew_consistency = {
                "directors": directors_consistency,
                "writers": writers_consistency
            }

        # 3. Plot rating variance by genre
        genre_stats = genre_consistency.toPandas()
        genre_stats.plot(
            kind='bar', x='genre', y='rating_stddev',
            title='Rating Variance by Genre', legend=False,
            figsize=(12, 6)
        )
        plt.xlabel("Genre")
        plt.ylabel("Rating Variance")
        plt.grid(True)
        plt.show()

  
        # Return results
        return {
            "genre_consistency": genre_consistency,
            "crew_consistency": crew_consistency
        }
    def analyze_runtime_perception(titles_df, rating_df):
        """
        Analyze the effect of runtime on audience perception.

        :param titles_path: Path to title.basics.tsv.gz
        :param ratings_path: Path to title.ratings.tsv.gz
        :return: DataFrame with runtime analysis results
        """
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Runtime Perception Analysis") \
            .getOrCreate()

        # Filter and preprocess titles
        titles_df = titles_df.filter((col('isAdult') == '0') & (col('runtimeMinutes') != '\\N'))
        titles_df = titles_df.withColumn('runtimeMinutes', col('runtimeMinutes').cast(IntegerType()))
        rating_df = rating_df.withColumn('averageRating', col('averageRating').cast(DoubleType()))
        rating_df = rating_df.withColumn('numVotes', col('numVotes').cast(IntegerType()))

        # Join datasets
        titles_with_ratings = titles_df.join(rating_df, 'tconst', 'left')

        # Create runtime bins
        titles_with_bins = titles_with_ratings.withColumn(
            'runtime_bin',
            when(col('runtimeMinutes') < 30, '<30 min')
            .when(col('runtimeMinutes') <= 60, '30-60 min')
            .when(col('runtimeMinutes') <= 90, '60-90 min')
            .when(col('runtimeMinutes') <= 120, '90-120 min')
            .otherwise('>120 min')
        )

        # Analyze runtime effects
        runtime_analysis = titles_with_bins.groupBy('runtime_bin') \
            .agg(
                avg('averageRating').alias('avg_rating'),
                stddev('averageRating').alias('rating_stddev'),
                avg('numVotes').alias('avg_votes'),
                count('*').alias('num_titles')
            ) \
            .orderBy('runtime_bin')

        # Visualize runtime vs. average rating
        runtime_stats = runtime_analysis.toPandas()
        plt.figure(figsize=(10, 6))
        plt.bar(runtime_stats['runtime_bin'], runtime_stats['avg_rating'], color='skyblue', edgecolor='black')
        plt.title("Average Rating by Runtime Bin")
        plt.xlabel("Runtime Bin")
        plt.ylabel("Average Rating")
        plt.grid(axis='y')
        plt.show()

        # Visualize runtime vs. average votes
        plt.figure(figsize=(10, 6))
        plt.bar(runtime_stats['runtime_bin'], runtime_stats['avg_votes'], color='lightgreen', edgecolor='black')
        plt.title("Average Number of Votes by Runtime Bin")
        plt.xlabel("Runtime Bin")
        plt.ylabel("Average Votes")
        plt.grid(axis='y')
        plt.show()

        return runtime_analysis
    def analyze_audience_preferences_by_genre(titles_df, rating_df):
        """
        Analyze audience preferences across genres based on ratings and vote counts.

        :param titles_path: Path to title.basics.tsv.gz
        :param ratings_path: Path to title.ratings.tsv.gz
        :return: DataFrame with genre preference metrics
        """
   

        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Audience Preferences by Genre") \
            .getOrCreate()

        # Filter and preprocess titles
        titles_df = titles_df.filter((col('isAdult') == '0') & (col('genres') != '\\N'))
        titles_df = titles_df.withColumn('genres', split(col('genres'), ','))  # Split genres into array
        rating_df = rating_df.withColumn('averageRating', col('averageRating').cast(DoubleType()))
        rating_df = rating_df.withColumn('numVotes', col('numVotes').cast('integer'))

        # Join datasets
        titles_with_ratings = titles_df.join(rating_df, 'tconst', 'left')

        # Explode genres for analysis
        titles_with_genres = titles_with_ratings.withColumn('genre', explode(col('genres')))

        # Analyze preferences by genre
        genre_preferences = titles_with_genres.groupBy('genre') \
            .agg(
                avg('averageRating').alias('avg_rating'),
                stddev('averageRating').alias('rating_stddev'),
                avg('numVotes').alias('avg_votes'),
                count('*').alias('num_titles')
            ) \
            .orderBy(desc('avg_rating'))

        # Visualize audience preferences by genre
        genre_stats = genre_preferences.toPandas()

        # Plot average ratings by genre
        genre_stats.sort_values('avg_rating', ascending=False, inplace=True)
        genre_stats.plot(
            kind='bar', x='genre', y='avg_rating',
            title='Average Rating by Genre', legend=False,
            figsize=(12, 6)
        )
        plt.xlabel("Genre")
        plt.ylabel("Average Rating")
        plt.grid(True)
        plt.show()

        # Plot average votes by genre
        genre_stats.sort_values('avg_votes', ascending=False, inplace=True)
        genre_stats.plot(
            kind='bar', x='genre', y='avg_votes',
            title='Average Votes by Genre', legend=False,
            figsize=(12, 6)
        )
        plt.xlabel("Genre")
        plt.ylabel("Average Votes")
        plt.grid(True)
        plt.show()

        return genre_preferences
    def identify_consistent_genres(titles_df, rating_df):
        """
        Identify genres with the most consistent and inconsistent ratings.

        :param titles_path: Path to title.basics.tsv.gz
        :param ratings_path: Path to title.ratings.tsv.gz
        :return: DataFrames for consistent and inconsistent genres
        """
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Consistent/Inconsistent Genre Ratings") \
            .getOrCreate()

        # Filter and preprocess titles
        titles_df = titles_df.filter((col('isAdult') == '0') & (col('genres') != '\\N'))
        titles_df = titles_df.withColumn('genres', split(col('genres'), ','))  # Split genres into array
        rating_df = rating_df.withColumn('averageRating', col('averageRating').cast(DoubleType()))
        rating_df = rating_df.withColumn('numVotes', col('numVotes').cast('integer'))

        # Join datasets
        titles_with_ratings = titles_df.join(rating_df, 'tconst', 'left')

        # Explode genres for analysis
        titles_with_genres = titles_with_ratings.withColumn('genre', explode(col('genres')))

        # Calculate consistency metrics by genre
        genre_consistency = titles_with_genres.groupBy('genre') \
            .agg(
                avg('averageRating').alias('avg_rating'),
                stddev('averageRating').alias('rating_stddev'),
                count('*').alias('num_titles')
            ) \
            .filter(col('num_titles') > 50)  # Filter for genres with significant data
        
        # Most consistent genres (lowest standard deviation)
        most_consistent_genres = genre_consistency.orderBy(asc('rating_stddev')).limit(5)

        # Most inconsistent genres (highest standard deviation)
        most_inconsistent_genres = genre_consistency.orderBy(desc('rating_stddev')).limit(5)

        # Show bar charts for consistency
        consistent_df = most_consistent_genres.toPandas()
        inconsistent_df = most_inconsistent_genres.toPandas()

        # Plotting most consistent genres
        consistent_df.plot(
            kind='bar', x='genre', y='rating_stddev',
            title='Most Consistent Genres (Low Std Dev)', legend=False,
            figsize=(12, 6), color='skyblue', edgecolor='black'
        )
        plt.xlabel("Genre")
        plt.ylabel("Rating Standard Deviation")
        plt.grid(axis='y')
        plt.show()

        # Plotting most inconsistent genres
        inconsistent_df.plot(
            kind='bar', x='genre', y='rating_stddev',
            title='Most Inconsistent Genres (High Std Dev)', legend=False,
            figsize=(12, 6), color='orange', edgecolor='black'
        )
        plt.xlabel("Genre")
        plt.ylabel("Rating Standard Deviation")
        plt.grid(axis='y')
        plt.show()

        return {
            "most_consistent_genres": most_consistent_genres,
            "most_inconsistent_genres": most_inconsistent_genres
        }
    def compare_ratings_across_regions(akas_df, rating_df):
        """
        Compare average ratings across different regions.

        :param akas_path: Path to title.akas.tsv.gz
        :param ratings_path: Path to title.ratings.tsv.gz
        :return: DataFrame with regional rating comparison
        """
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Regional Ratings Comparison") \
            .getOrCreate()

        # Filter relevant data
        akas_df = akas_df.filter((col('region') != '\\N') & (col('region') != 'None'))
        rating_df = rating_df.withColumn('averageRating', col('averageRating').cast('double'))
        rating_df = rating_df.withColumn('numVotes', col('numVotes').cast('integer'))

        # Join regional data with ratings
        regional_ratings = akas_df.join(rating_df, 'titleId', 'inner')

        # Aggregate average ratings and number of titles per region
        region_comparison = regional_ratings.groupBy('region') \
            .agg(
                avg('averageRating').alias('avg_rating'),
                count('*').alias('num_titles')
            ) \
            .orderBy(desc('avg_rating'))

        # Filter regions with a significant number of titles
        significant_regions = region_comparison.filter(col('analyze_rating_consistency'))
        top_regions = significant_regions.limit(10)

        # Plotting regional comparison
        top_regions_pd = top_regions.toPandas()

        # Bar chart of average ratings by region
        top_regions_pd.plot(
            kind='bar', x='region', y='avg_rating',
            title='Top Regions by Average Rating', legend=False,
            figsize=(12, 6), color='skyblue', edgecolor='black'
        )
        plt.xlabel("Region")
        plt.ylabel("Average Rating")
        plt.grid(axis='y')
        plt.show()

        return significant_regions
    def analyze_content_type_trends(titles_df):
        """
        Analyze changes in content type production over time.

        :param titles_path: Path to title.basics.tsv.gz
        :return: DataFrame with content type trends over time
        """
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Content Type Trends Analysis") \
            .getOrCreate()

        # Filter out invalid or incomplete data
        titles_df = titles_df.filter((col('startYear') != '\\N') & (col('titleType') != '\\N'))
        titles_df = titles_df.withColumn('startYear', col('startYear').cast('int'))

        # Group by content type and start year, counting the number of titles
        content_type_trends = titles_df.groupBy('startYear', 'titleType') \
            .agg(count('*').alias('num_titles')) \
            .orderBy('startYear', 'titleType')

        # Collect data for visualization
        trends_pd = content_type_trends.toPandas()

        # Plot trends
        plt.figure(figsize=(14, 8))
        for content_type in trends_pd['titleType'].unique():
            subset = trends_pd[trends_pd['titleType'] == content_type]
            plt.plot(subset['startYear'], subset['num_titles'], label=content_type)

        plt.title('Changes in Content Type Production Over Time')
        plt.xlabel('Year')
        plt.ylabel('Number of Titles')
        plt.legend(title="Content Type")
        plt.grid(True)
        plt.show()

        return content_type_trends