import setuptools.dist
import matplotlib.pyplot as plt
import seaborn as sns
class MovieVisualizer:
    @staticmethod
    def plot_genre_distribution(genre_df):
        """Vẽ biểu đồ phân bố thể loại"""
        plt.figure(figsize=(12, 6))
        data = genre_df.toPandas()
        sns.barplot(data=data, x='genre', y='count')
        plt.title("Phân bố thể loại phim")
        plt.xticks(rotation=45)
        plt.xlabel("Thể loại")
        plt.ylabel("Số lượng phim")
        return plt.gcf()
    
    @staticmethod
    def plot_job_distribution(job_df):
        """Vẽ biểu đồ phân bố vai trò trong đoàn phim"""
        plt.figure(figsize=(15, 8))
        data = job_df.toPandas()
        sns.barplot(data=data, x='category', y='count', hue='job')
        plt.title("Phân bố vai trò trong đoàn làm phim")
        plt.xticks(rotation=45)
        plt.xlabel("Danh mục")
        plt.ylabel("Số lượng")
        return plt.gcf()
    
    @staticmethod
    def plot_director_productivity(director_df):
        """Vẽ biểu đồ top đạo diễn"""
        plt.figure(figsize=(12, 6))
        data = director_df.toPandas()
        sns.barplot(data=data, x='nconst', y='count')
        plt.title("Top đạo diễn có nhiều phim nhất")
        plt.xticks(rotation=45)
        plt.xlabel("ID đạo diễn")
        plt.ylabel("Số lượng phim")
        return plt.gcf()
    
    @staticmethod
    def plot_collaboration_network(collab_df):
        """Vẽ biểu đồ mạng lưới cộng tác"""
        plt.figure(figsize=(12, 8))
        data = collab_df.toPandas()
        plt.scatter(data['director_id'], data['producer_id'], s=data['count']*10)
        plt.title("Mạng lưới cộng tác Đạo diễn - Nhà sản xuất")
        plt.xlabel("ID đạo diễn")
        plt.ylabel("ID nhà sản xuất")
        return plt.gcf()
    
    @staticmethod
    def plot_multi_role_distribution(role_df):
        """Vẽ biểu đồ phân bố người đa vai trò"""
        plt.figure(figsize=(12, 6))
        data = role_df.toPandas()
        sns.barplot(data=data, x='nconst', y='num_roles')
        plt.title("Top người đảm nhiệm nhiều vai trò")
        plt.xticks(rotation=45)
        plt.xlabel("ID người")
        plt.ylabel("Số vai trò")
        return plt.gcf()