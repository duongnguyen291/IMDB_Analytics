from pyspark.sql.functions import col, when

class DataCleaner:
    @staticmethod
    def handle_nulls(df, columns, strategy="drop"):
        if strategy == "drop":
            return df.dropna(subset=columns)
        elif strategy == "fill":
            return df.fillna(0, subset=columns)
    
    @staticmethod
    def remove_duplicates(df, key_columns):
        return df.dropDuplicates(subset=key_columns)