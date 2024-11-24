import os

# Define the project structure
project_structure = {
    "imdb_analytics": {
        "config": ["__init__.py", "spark_config.py", "app_config.py"],
        "data": ["__init__.py", "data_loader.py", "data_writer.py"],
        "preprocessing": ["__init__.py", "cleaner.py", "transformer.py"],
        "analysis": ["__init__.py", "basic_analysis.py", "advanced_analysis.py", "utils.py"],
        "visualization": ["__init__.py", "plotly_charts.py", "matplotlib_charts.py"],
        "models": ["__init__.py", "train.py", "predict.py", "evaluation.py"],
        "api": ["__init__.py", "routes.py", "schemas.py"],
        "utils": ["__init__.py", "logger.py", "helpers.py"],
        "tests": ["__init__.py", "test_data.py", "test_analysis.py", "test_models.py"],
        "notebooks": ["exploration.ipynb"],
    },
    "imdb_analytics_files": ["main.py", "requirements.txt", "README.md"],
}

# Function to create directories and files
def create_project_structure(base_path, structure):
    for key, value in structure.items():
        if isinstance(value, dict):
            # Create folder and recurse
            folder_path = os.path.join(base_path, key)
            os.makedirs(folder_path, exist_ok=True)
            create_project_structure(folder_path, value)
        elif isinstance(value, list):
            # Create folder and files
            folder_path = os.path.join(base_path, key)
            os.makedirs(folder_path, exist_ok=True)
            for file in value:
                file_path = os.path.join(folder_path, file)
                with open(file_path, "w") as f:
                    f.write("")  # Create an empty file
        elif isinstance(value, str):
            # Create a single file
            file_path = os.path.join(base_path, key)
            with open(file_path, "w") as f:
                f.write("")  # Create an empty file

# Create the project structure
base_directory = os.getcwd()  # Change to your desired base path
create_project_structure(base_directory, project_structure)

print(f"Project structure created successfully at {base_directory}")
