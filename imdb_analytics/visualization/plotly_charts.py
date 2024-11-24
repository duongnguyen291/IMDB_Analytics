import plotly.express as px

class MovieVisualizer:
    @staticmethod
    def plot_genre_distribution(genre_df):
        fig = px.bar(
            genre_df.toPandas(),
            x="genre",
            y="count",
            title="Genre Distribution"
        )
        return fig
    
    @staticmethod
    def plot_rating_trend(rating_df):
        fig = px.line(
            rating_df.toPandas(),
            x="year",
            y="avg_rating",
            title="Rating Trend Over Years"
        )
        return fig