from pyspark.sql import SparkSession

# Định nghĩa constants
HDFS_HOST = "hdfs://localhost:9000"  
HDFS_PATH = f"{HDFS_HOST}/hadoop/data/parquet/"

def create_spark_session(app_name="IMDb Analytics"):
    """
    Tạo và cấu hình SparkSession với các thiết lập phù hợp.
    
    Parameters:
        app_name (str): Tên của ứng dụng Spark
        
    Returns:
        SparkSession: SparkSession đã được cấu hình
    """
    return SparkSession.builder \
        .appName(app_name) \
        .master("spark://localhost:7077") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.hadoop.fs.defaultFS", HDFS_HOST) \
        .config("spark.sql.warehouse.dir", f"{HDFS_HOST}/user/hive/warehouse") \
        .config("spark.executor.cores", "1") \
        .config("spark.driver.cores", "1") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.executor.instances", "2")   \
        .getOrCreate()
        # .config("spark.dynamicAllocation.minExecutors", "2") \
        # .config("spark.dynamicAllocation.maxExecutors", "2") \
        #  .config("spark.dynamicAllocation.enabled", "true") \
       

# Hàm tiện ích để kiểm tra kết nối HDFS
def test_hdfs_connection(spark):
    """
    Kiểm tra kết nối tới HDFS bằng cách đọc thử một file parquet
    
    Parameters:
        spark (SparkSession): SparkSession đã được khởi tạo
        
    Returns:
        bool: True nếu kết nối thành công, False nếu thất bại
    """
    try:
        # Thử đọc một file parquet bất kỳ
        test_df = spark.read.parquet(f"{HDFS_PATH}/title_basics_parquet")
        test_df.printSchema()
        return True
    except Exception as e:
        print(f"Lỗi kết nối HDFS: {str(e)}")
        return False