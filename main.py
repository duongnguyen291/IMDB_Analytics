from config.spark_config import create_spark_session
from analysis.basic_analysis import MovieAnalyzer
from visualization.plotly_charts import MovieVisualizer
from data.data_loader import IMDbDataLoader

def main():
    # Initialize Spark
    spark = create_spark_session()
    
    # Load data
    loader = IMDbDataLoader(spark, "/hadoop/data/parquet/")
    movies_df = loader.load_titles()
    ratings_df = loader.load_ratings()
    
    # Analysis
    analyzer = MovieAnalyzer(movies_df, ratings_df)
    genre_dist = analyzer.get_genre_distribution()
    
    # Visualization
    visualizer = MovieVisualizer()
    genre_plot = visualizer.plot_genre_distribution(genre_dist)
    genre_plot.show()

if __name__ == "__main__":
    main()