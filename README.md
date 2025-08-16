# Amazon Reviews Analytics

This project analyzes Amazon product and review data using PySpark. It is designed to run on AWS EMR and provides a series of data processing and machine learning tasks for large-scale analytics.

## Features
- Data aggregation and joining of product and review datasets
- Feature engineering (category extraction, sales rank, related products)
- Imputation of missing values (mean, median, unknown handling)
- Text processing and Word2Vec embedding for product titles
- Categorical encoding (StringIndexer, OneHotEncoder, PCA)
- Regression modeling with Decision Trees
- Model evaluation and hyperparameter tuning

## File Structure
- `21.py`: Main script containing all data processing and ML tasks
- `DSC102_PA2.pdf`: Project instructions and details
- `utilities.py`: Utility functions and constants (e.g., random seed)

## Requirements
- Python 3.x
- PySpark (compatible with AWS EMR)
- pandas, numpy

## Usage
1. Upload the scripts to your AWS EMR cluster.
2. Run `21.py` as the main entry point. The script expects Spark DataFrames as input for review and product data.
3. Each `task_X` function performs a specific analytics or ML task and saves results using the provided `data_io` interface.

## Tasks Overview
- **Task 1**: Aggregate review ratings and join with product data
- **Task 2**: Extract product categories and best sales rank
- **Task 3**: Analyze prices of related products
- **Task 4**: Impute missing prices and handle unknown titles
- **Task 5**: Generate Word2Vec embeddings for product titles
- **Task 6**: Encode categories and apply PCA
- **Task 7**: Train and evaluate a Decision Tree regressor
- **Task 8**: Hyperparameter tuning for Decision Tree depth

## Notes
- Only use dependencies available on AWS EMR.
- The code is modular; each task can be run independently.
- Results are saved using the `data_io.save()` method for each task.

## License
This project is for educational purposes.
