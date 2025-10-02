# Amazon Reviews Analytics

This project explores the dynamics of Amazon product ratings and reviews through a distributed data pipeline built with PySpark. By processing 12GB of real-world data, the pipeline uncovers patterns in consumer behavior, product features, and pricing strategies. Designed for scalability, it leverages the power of distributed computing to handle complex feature engineering and machine learning tasks efficiently.

## Project Highlights
This pipeline goes beyond basic data processing to provide actionable insights into e-commerce trends. It combines advanced feature engineering techniques with robust machine learning workflows to answer questions like:
- How do product categories influence ratings?
- What role does pricing play in consumer perception?
- Can we predict product success based on metadata alone?

The project was developed as part of UCSD’s Systems for Scalable Analytics coursework, offering hands-on experience with big data tools and techniques.

## Key Features
### Data Transformation
- Flattened hierarchical JSON structures to extract meaningful fields (e.g., categories, salesRank, related products)
- Performed joins between product and review datasets to enrich the data
- Addressed missing values with statistical imputations (mean/median pricing, default titles)

### Feature Engineering
- **Text Analysis**: Generated Word2Vec embeddings to capture semantic relationships in product titles
- **Categorical Encoding**: Transformed categories using StringIndexer and OneHotEncoder, followed by PCA for dimensionality reduction
- **Statistical Summaries**: Computed dense vector summaries for encoded features

### Machine Learning
- Built Decision Tree Regressors to predict product ratings
- Conducted hyperparameter tuning to optimize model performance
- Implemented caching and schema pruning to enhance computational efficiency

## Tools and Technologies
- **Programming Language**: Python
- **Frameworks**: PySpark (SQL, MLlib, DataFrame API)
- **Platforms**: UCSD DSMLP (Kubernetes-backed Spark cluster), Jupyter Notebook
- **Data**: 12GB Amazon product and review dataset

## File Overview
- `amazon_reviews_analytics.py`: Core script for data processing and modeling
- `DSC102_PA2.pdf`: Project guidelines and requirements
- `utilities.py`: Helper functions and constants

## How to Use
1. **Setup**: Deploy the scripts to a Spark cluster (e.g., AWS EMR or UCSD DSMLP).
2. **Input Data**: Provide Spark DataFrames for product and review datasets.
3. **Run Pipeline**: Execute `amazon_reviews_analytics.py` to process data and train models.
4. **Output**: Results are saved in a structured format for further analysis.

## Analytical Tasks
1. **Aggregate Reviews**: Combine product and review data to calculate rating statistics.
2. **Extract Categories**: Parse and analyze hierarchical category data.
3. **Analyze Related Products**: Study pricing trends in "also_viewed" items.
4. **Handle Missing Data**: Impute null values in pricing and titles.
5. **Generate Embeddings**: Train Word2Vec models on product titles.
6. **Encode Features**: Apply categorical encoding and PCA.
7. **Train Models**: Build and evaluate Decision Tree regressors.
8. **Optimize Models**: Tune hyperparameters for improved accuracy.

## Insights and Observations
- **Category Trends**: Certain categories exhibit higher rating variability, reflecting diverse consumer expectations.
- **Pricing Patterns**: Missing prices often align with specific product types, hinting at strategic pricing decisions.
- **Semantic Relationships**: Word2Vec embeddings reveal unexpected connections between products, such as shared attributes across categories.

## Notes
- The pipeline is optimized for distributed environments and handles large-scale data efficiently.
- Modular design allows for independent execution of tasks.
- Results are stored in a format compatible with downstream analytics tools.

## License
This project is for educational purposes.
