from imdb_analytics.config.spark_config import create_spark_session
from imdb_analytics.analysis.basic_analysis import MovieAnalyzer
from imdb_analytics.visualization.plotly_charts import MovieVisualizer
from imdb_analytics.data.data_loader import IMDbDataLoader
from imdb_analytics.analysis.advanced_used import AdvancedUsage
def main_1():
  # When have 2 workers
    spark = create_spark_session()

    loader = IMDbDataLoader(spark, "hdfs:///hadoop/data/parquet/")

    # Sử dụng SparkContext để lấy thông tin về các executors
    workers_info = spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()

    # Đếm số lượng workers (executors)
    num_workers = len(workers_info) - 1

    # In ra số lượng workers và thông tin chi tiết
    print(f"Số lượng workers đang hoạt động: {num_workers}")
    for worker in workers_info:
        print(f"Executor Info: {worker}")
      
def main_2():
    spark = create_spark_session()
    loader = IMDbDataLoader(spark, "hdfs:///hadoop/data/parquet/")
    titles_df = loader.load_titles()
    rating_df = loader.load_ratings()
    names_df = loader.load_names()
    akas_df = loader.load_akas()
    episode_df = loader.load_episodes()
    principals_df = loader.load_principals()
    crew_df = loader.load_crews()
    principal_df = loader.load_principals()

        # 8. Analyze rating consistency (for genres and crew) when run with 1 workers
    AdvancedUsage.analyze_rating_consistency_usage(
        titles_df, rating_df, crew_df, output_crew_insights=True
    )
    # Sử dụng SparkContext để lấy thông tin về các executors
    workers_info = spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()

    # Đếm số lượng workers (executors)
    num_workers = len(workers_info) - 1

    # In ra số lượng workers và thông tin chi tiết
    print(f"Số lượng workers đang hoạt động: {num_workers}")
    for worker in workers_info:
        print(f"Executor Info: {worker}")

if __name__ == "__main__":
    main_1()