import setuptools.dist
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql import SparkSession
from analysis.advanced_analysis import AdvancedAnalysis
from pyspark.sql.functions import (
    explode, col, count, desc, avg, 
    sum , when, concat_ws,
    split, year, struct, dense_rank,
    var_pop,  
)
class AdvancedUsage:
    def top_rated_titles_usage(titles_df,rating_df,crew_df,principals_df):
        top_rated= AdvancedAnalysis.analyze_top_rated_titles(titles_df,rating_df,crew_df,principals_df)

        # Top-rated titles by genre
        top_rated_genre = top_rated["top_rated_genre"]
        top_rated_genre.show()

        # Top-rated titles by title type (movie vs tvseries)
        top_rated_title_type = top_rated["top_rated_title_type"]
        top_rated_title_type.show()

        # Top-rated titles overall
        top_rated_overall = top_rated["top_rated_overall"]
        top_rated_overall.show()
    def analyze_rating_distribution_usage(titles_df,rating_df,crew_df,principals_df):
        rating_distribution = AdvancedAnalysis.analyze_rating_distribution(
        titles_df, 
        rating_df, 
        crew_df, 
        principals_df
        )
        rating_distribution["rating_distribution"].show()

# Show rating distribution by ranges (e.g., 0-2, 2-4, etc.)
        rating_distribution["rating_ranges"].show()
    def analyze_ratings_by_title_type_usage(titles_df,rating_df,crew_df,principals_df):
        avg_rating_by_title_type = AdvancedAnalysis.analyze_ratings_by_title_type(
        titles_df, 
        rating_df, 
        crew_df, 
        principals_df
        )
        avg_rating_by_title_type.show()
    def analyze_director_writer_performance_usage(titles_df, rating_df, crew_df, principals_df):
        performance_metrics = AdvancedAnalysis.analyze_director_writer_performance(
        titles_df, 
        rating_df, 
        crew_df, 
        principals_df
        )
        performance_metrics.show()
    def analyze_crew_performance_and_ratings_usage(
        titles_df, 
        rating_df, 
        crew_df, 
        principals_df
    ):    
       performance_metrics = AdvancedAnalysis.analyze_crew_performance_and_ratings(
        titles_df, 
        rating_df, 
        crew_df, 
        principals_df
        ) 
       performance_metrics.show()
    def identify_consistently_high_performing_crew_usage(
        titles_df, 
        rating_df, 
        crew_df, 
        principals_df,
    ):
        high_performers = AdvancedAnalysis.identify_consistently_high_performing_crew(
        titles_df, 
        rating_df, 
        crew_df, 
        principals_df,
        avg_rating_threshold=8.0,   # Consider average rating >= 8.0 as high-performing
        min_titles_threshold=5      # Consider only those with at least 5 titles
        )
        high_performers.show()
    def analyze_rating_variance_by_title_type_usage(
        titles_df, 
        rating_df
    ):
        rating_variance = AdvancedAnalysis.analyze_rating_variance_by_title_type(titles_df, rating_df)
        rating_variance.show()
    def analyze_rating_consistency_usage(
        titles_df, 
        rating_df, 
        crew_df, 
        output_crew_insights #TRUE/FALSE
    ):
        results = AdvancedAnalysis.analyze_rating_consistency(
        titles_df,
        rating_df,
        crew_df,
        output_crew_insights  # Include insights for crew
    )
        start_time = time.time()
        genre_consistency_df = results["genre_consistency"]
        crew_consistency = results.get("crew_consistency")

        # Show top genres by consistency
        print("Top Genres by Consistency:")
        genre_consistency_df.show()

        if crew_consistency:
            print("Top Directors by Consistency:")
            crew_consistency["directors"].show()

            print("Top Writers by Consistency:")
            crew_consistency["writers"].show()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Execution time: {elapsed_time:.2f} seconds")
        
    def analyze_runtime_perception_usage(titles_df, rating_df):
        runtime_results = AdvancedAnalysis.analyze_runtime_perception(titles_df, rating_df)
        runtime_results.show()
    def analyze_audience_preferences_by_genre_usage(titles_df, rating_df):
        start_time = time.time()
        genre_preferences = AdvancedAnalysis.analyze_audience_preferences_by_genre(titles_df, rating_df)
        genre_preferences.show()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Execution time: {elapsed_time:.2f} seconds")
    def identify_consistent_genres_usage(titles_df, rating_df):
        results = AdvancedAnalysis.identify_consistent_genres(titles_df, rating_df)

        # Access results
        print("Most Consistent Genres:")
        results["most_consistent_genres"].show()

        print("Most Inconsistent Genres:")
        results["most_inconsistent_genres"].show()
    def compare_ratings_across_regions_usage(akas_df, rating_df):
        regional_ratings = AdvancedAnalysis.compare_ratings_across_regions(akas_df, rating_df)
        print("Regional Ratings Comparison:")
        regional_ratings.show()
    def analyze_content_type_trends_usage(titles_df):
        content_type_trends = AdvancedAnalysis.analyze_content_type_trends(titles_df)
        print("Content Type Trends Over Time:")
        content_type_trends.show()