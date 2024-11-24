# IMDB ANALYTICS
## PROJECT STRUCTURE
```
imdb_analytics/
│
├── config/
│   ├── __init__.py
│   ├── spark_config.py      # Cấu hình Spark Session
│   └── app_config.py        # Các config khác (path, params,...)
│
├── data/
│   ├── __init__.py
│   ├── data_loader.py       # Load data từ parquet
│   └── data_writer.py       # Ghi data ra các format khác
│
├── preprocessing/
│   ├── __init__.py
│   ├── cleaner.py          # Xử lý null, duplicate,...
│   └── transformer.py      # Transform data types, features
│
├── analysis/
│   ├── __init__.py
│   ├── basic_analysis.py   # Các phân tích cơ bản
│   ├── advanced_analysis.py # Các phân tích nâng cao
│   └── utils.py            # Các hàm tiện ích phân tích
│
├── visualization/
│   ├── __init__.py
│   ├── plotly_charts.py    # Vẽ biểu đồ với plotly 
│   └── matplotlib_charts.py # Vẽ biểu đồ với matplotlib
│
├── models/
│   ├── __init__.py
│   ├── train.py           # Training models
│   ├── predict.py         # Prediction
│   └── evaluation.py      # Đánh giá model
│
├── api/
│   ├── __init__.py
│   ├── routes.py          # API endpoints
│   └── schemas.py         # Pydantic schemas
│
├── utils/
│   ├── __init__.py
│   ├── logger.py          # Logging
│   └── helpers.py         # Các hàm helper
│
├── tests/
│   ├── __init__.py
│   ├── test_data.py
│   ├── test_analysis.py
│   └── test_models.py
│
├── notebooks/
│   └── exploration.ipynb  # Jupyter notebooks cho EDA
│
├── main.py               # Entry point
├── requirements.txt      # Dependencies
└── README.md            # Documentation
```