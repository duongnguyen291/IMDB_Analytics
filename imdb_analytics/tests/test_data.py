from ..config.spark_config import create_spark_session
from ..data.data_loader import IMDbDataLoader
# Tạo SparkSession
spark = create_spark_session()
# Đọc file Parquet
loader = IMDbDataLoader(spark, "hdfs:///hadoop/data/parquet/")

titles_df = loader.load_titles()
rating_df = loader.load_ratings()
titles_df.printSchema(5)
titles_df.show(5)