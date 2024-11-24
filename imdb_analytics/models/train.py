from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor

class MovieRatingPredictor:
    def __init__(self):
        self.indexer = StringIndexer(
            inputCol="genres",
            outputCol="genreIndex"
        )
        self.encoder = OneHotEncoder(
            inputCols=["genreIndex"],
            outputCols=["genreVec"]
        )
        self.rf = RandomForestRegressor(
            featuresCol="genreVec",
            labelCol="averageRating"
        )
    
    def prepare_features(self, df):
        indexed = self.indexer.fit(df).transform(df)
        return self.encoder.fit(indexed).transform(indexed)