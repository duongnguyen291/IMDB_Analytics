# IMDb Analytics

## Overview
IMDb Analytics is a project aimed at storing, processing, and analyzing IMDb's publicly available non-commercial datasets. The project leverages distributed computing with Apache Spark to handle large datasets efficiently and provides a modular, extensible structure for various analyses, model training, and visualization.

## Project Structure

```plaintext
imdb_analytics/
│
├── config/
│   ├── __init__.py
│   ├── spark_config.py      # Configuration for Spark Session
│   └── app_config.py        # Other configurations (paths, parameters, etc.)
│
├── data/
│   ├── __init__.py
│   ├── data_loader.py       # Load data from Parquet or TSV
│   └── data_writer.py       # Write data to different formats
│
├── preprocessing/
│   ├── __init__.py
│   ├── cleaner.py          # Handle null values, duplicates, etc.
│   └── transformer.py      # Transform data types, generate features
│
├── analysis/
│   ├── __init__.py
│   ├── basic_analysis.py   # Basic analyses like descriptive statistics
│   ├── advanced_analysis.py # Advanced analyses like correlation and trend analysis
│   └── utils.py            # Utility functions for analysis
│
├── visualization/
│   ├── __init__.py
│   ├── plotly_charts.py    # Create interactive charts with Plotly
│   └── matplotlib_charts.py # Create visualizations with Matplotlib
│
├── models/
│   ├── __init__.py
│   ├── train.py           # Train machine learning models
│   ├── predict.py         # Perform predictions using trained models
│   └── evaluation.py      # Evaluate model performance
│
├── api/
│   ├── __init__.py
│   ├── routes.py          # API endpoints for external access
│   └── schemas.py         # Pydantic schemas for data validation
│
├── utils/
│   ├── __init__.py
│   ├── logger.py          # Centralized logging
│   └── helpers.py         # General-purpose helper functions
│
├── tests/
│   ├── __init__.py
│   ├── test_data.py        # Test cases for data handling
│   ├── test_analysis.py    # Test cases for analysis logic
│   └── test_models.py      # Test cases for models
│
├── notebooks/
│   └── exploration.ipynb  # Jupyter Notebook for exploratory data analysis
│
├── main.py                # Project entry point
├── requirements.txt       # Python dependencies
└── README.md              # Documentation
```

## Data Source
The datasets used in this project are publicly available non-commercial IMDb datasets.

## Dataset Files
The following datasets are downloaded and processed:
* **name.basics.tsv.gz**: Information about people in the film industry.
* **title.akas.tsv.gz**: Localized titles and alternative names for titles.
* **title.basics.tsv.gz**: General information about titles.
* **title.crew.tsv.gz**: Directors and writers associated with titles.
* **title.episode.tsv.gz**: Information on TV episodes and their series.
* **title.principals.tsv.gz**: Principal cast and crew for titles.
* **title.ratings.tsv.gz**: Ratings and votes for titles.

## Data Location
Files are downloaded from IMDb Datasets and stored in the HDFS directory: `/hadoop/data/`.

## Key Features

### 1. Data Processing
* Load IMDb TSV files into Parquet format for optimized storage and processing.
* Handle null values, duplicates, and transform data types for downstream analysis.

### 2. Analysis
* Perform basic and advanced analyses to extract meaningful insights (e.g., top-rated movies, genre trends, etc.).
* Modular functions to analyze ratings, crew contributions, and viewer preferences.

### 3. Visualization
* Generate insightful and interactive charts using Plotly.
* Create static, publication-quality visualizations with Matplotlib.

### 4. Modeling
* Train machine learning models for predictive tasks such as rating prediction.
* Evaluate models using industry-standard metrics.

### 5. API
* Expose data and analysis results via RESTful APIs for integration with other systems.

### 6. Testing
* Comprehensive test coverage for data processing, analysis, and modeling modules.

## How to Use

### 1. Prerequisites
* Python 3.8+
* Apache Spark
* Hadoop HDFS (for storage)

### 2. Setup
1. Clone the repository:
```bash
git clone https://github.com/duongnguyen291/IMDB_Analytics
cd imdb_analytics
```

2. Create a virtual environment and install dependencies:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

3. Configure Spark and application settings in `config/spark_config.py` and `config/app_config.py`.

4. Download IMDb datasets and upload them to HDFS:
```bash
wget -P /local/data/ https://datasets.imdbws.com/*.tsv.gz
hdfs dfs -put /local/data/*.tsv.gz /hadoop/data/
```

### 3. Running the Project
To execute the analysis pipeline:
```bash
python main.py
```

## License
This project is intended for **personal and non-commercial use**. Please comply with IMDb's Non-Commercial Licensing Terms when using their datasets.

## Contact
For questions or contributions, please contact:
* **Email**: 2901nguyendinhduong@gmail.com
* **GitHub**: duongnguyen291