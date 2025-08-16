import os
import pyspark.sql.functions as F
import pyspark.sql.types as T
from utilities import SEED
# import any other dependencies you want, but make sure only to use the ones
# availiable on AWS EMR

# ---------------- choose input format, dataframe or rdd ----------------------
INPUT_FORMAT = 'dataframe'  # change to 'rdd' if you wish to use rdd inputs
# -----------------------------------------------------------------------------
import numpy as np
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType
from pyspark.ml.feature import StringIndexer, OneHotEncoder, PCA
from pyspark.ml.stat import Summarizer
if INPUT_FORMAT == 'dataframe':
    import pyspark.ml as M
    import pyspark.sql.functions as F
    import pyspark.sql.types as T
    from pyspark.ml.regression import DecisionTreeRegressor
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml.feature import Word2Vec
if INPUT_FORMAT == 'koalas':
    import databricks.koalas as ks
elif INPUT_FORMAT == 'rdd':
    import pyspark.mllib as M
    from pyspark.mllib.feature import Word2Vec
    from pyspark.mllib.linalg import Vectors
    from pyspark.mllib.linalg.distributed import RowMatrix
    from pyspark.mllib.tree import DecisionTree
    from pyspark.mllib.regression import LabeledPoint
    from pyspark.mllib.linalg import DenseVector
    from pyspark.mllib.evaluation import RegressionMetrics


# ---------- Begin definition of helper functions, if you need any ------------

def clean_title(x):
        x = str(x)
        x = x.lower()
        return x.split(' ')

# -----------------------------------------------------------------------------


def task_1(data_io, review_data, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    asin_column = 'asin'
    overall_column = 'overall'
    # Outputs:
    mean_rating_column = 'meanRating'
    count_rating_column = 'countRating'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    
    review_aggregated = review_data.groupBy('asin').agg(F.avg(F.col('overall')).alias('meanRating'), F.count('asin').alias('countRating'))
    final_data = product_data.join(review_aggregated, on = 'asin', how = 'left')
    



    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    # Calculate the values programmaticly. Do not change the keys and do not
    # hard-code values in the dict. Your submission will be evaluated with
    # different inputs.
    # Modify the values of the following dictionary accordingly.
    res = {
        'count_total': final_data.count(),
        'mean_meanRating': final_data.select(F.avg('meanRating')).collect()[0][0],  
        'variance_meanRating': final_data.select(F.variance('meanRating')).collect()[0][0],
        'numNulls_meanRating': final_data.where(F.col('meanRating').isNull()).count(),  
        'mean_countRating': final_data.select(F.avg('countRating')).collect()[0][0],
        'variance_countRating': final_data.select(F.variance('countRating')).collect()[0][0],
        'numNulls_countRating': final_data.where(F.col('countRating').isNull()).count()  
    }
    # Modify res:




    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_1')
    return res
    # -------------------------------------------------------------------------


def task_2(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    salesRank_column = 'salesRank'
    categories_column = 'categories'
    asin_column = 'asin'
    # Outputs:
    category_column = 'category'
    bestSalesCategory_column = 'bestSalesCategory'
    bestSalesRank_column = 'bestSalesRank'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------

    product_data = product_data.withColumn("category", 
                                           F.when(
                                               (F.col("categories").isNotNull()) &
                                               (F.size(F.col("categories")) > 0) &
                                               (F.size(F.col("categories")[0]) > 0) &
                                               (F.col("categories")[0][0] != ""),
                                               F.col("categories")[0][0]
                                           ))
    product_data = product_data.withColumn("bestSalesCategory",
                                          F.when(
                                              (F.col("salesRank").isNotNull()) &
                                              (F.size(F.map_keys(F.col("salesRank"))) > 0),
                                              F.map_keys(F.col("salesRank"))[0]
                                          ))
                                           
                                           
    product_data = product_data.withColumn("bestSalesRank", 
                                          F.when(
                                              (F.col("salesRank").isNotNull()) &
                                              (F.size(F.map_values(F.col("salesRank"))) > 0),
                                              F.map_values(F.col("salesRank"))[0]
                                          ))




    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_bestSalesRank': None,
        'variance_bestSalesRank': None,
        'numNulls_category': None,
        'countDistinct_category': None,
        'numNulls_bestSalesCategory': None,
        'countDistinct_bestSalesCategory': None
    }
    # Modify res:

    res['count_total'] = product_data.count()
    res['mean_bestSalesRank'] = float(product_data.select(F.mean("bestSalesRank")).first()[0])
    res['variance_bestSalesRank'] = float(product_data.select(F.variance("bestSalesRank")).first()[0])
    res['numNulls_category'] = product_data.select(F.sum(F.col("category").isNull().cast("int"))).first()[0]
    res['countDistinct_category'] = product_data.select(F.countDistinct(F.col("category"))).first()[0]
    res['numNulls_bestSalesCategory'] = product_data.select(F.sum(F.col("bestSalesCategory").isNull().cast("int"))).first()[0]
    res['countDistinct_bestSalesCategory'] = product_data.select(F.countDistinct(F.col("bestSalesCategory"))).first()[0]



    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_2')
    return res
    # -------------------------------------------------------------------------


def task_3(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    asin_column = 'asin'
    price_column = 'price'
    attribute = 'also_viewed'
    related_column = 'related'
    # Outputs:
    meanPriceAlsoViewed_column = 'meanPriceAlsoViewed'
    countAlsoViewed_column = 'countAlsoViewed'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------

    product_data = product_data.select("asin", "related", "price")
    
    original = product_data.select("asin").dropDuplicates()
    
    joiner = product_data.select("asin", "price").dropDuplicates(["asin"])
    joiner = joiner.withColumnRenamed("asin", "related_product").withColumnRenamed("price", "related_price")
    
    product_data = product_data.withColumn("also_viewed", F.col("related")['also_viewed'])
    product_data = product_data.withColumn("related_ID", F.explode("also_viewed"))

    product_data = product_data.join(
        joiner,
        on = product_data['related_ID'] == joiner['related_product'],
        how = 'left'
    )
    
    product_data = product_data.withColumn("has_price", F.col("related_price").isNotNull().cast("int"))
    
    grouped = product_data.repartition(16, "asin").groupby("asin").agg(
        F.mean("related_price").alias("meanPriceAlsoViewed"),
        F.count("related_ID").alias("countAlsoViewed")
    )

    final = original.join(grouped, on="asin", how="left")



    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_meanPriceAlsoViewed': None,
        'variance_meanPriceAlsoViewed': None,
        'numNulls_meanPriceAlsoViewed': None,
        'mean_countAlsoViewed': None,
        'variance_countAlsoViewed': None,
        'numNulls_countAlsoViewed': None
    }
    # Modify res:

    res['count_total'] = final.count()
    res['mean_meanPriceAlsoViewed'] = float(final.select(F.mean("meanPriceAlsoViewed")).first()[0])
    res['variance_meanPriceAlsoViewed'] = float(final.select(F.variance("meanPriceAlsoViewed")).first()[0])
    res['numNulls_meanPriceAlsoViewed'] = final.select(F.sum(F.col("meanPriceAlsoViewed").isNull().cast("int"))).first()[0]
    res['mean_countAlsoViewed'] = float(final.select(F.mean("countAlsoViewed")).first()[0])
    res['variance_countAlsoViewed'] = float(final.select(F.variance("countAlsoViewed")).first()[0])
    res['numNulls_countAlsoViewed'] = final.select(F.sum(F.col("countAlsoViewed").isNull().cast("int"))).first()[0]



    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_3')
    return res
    # -------------------------------------------------------------------------


def task_4(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    price_column = 'price'
    title_column = 'title'
    # Outputs:
    meanImputedPrice_column = 'meanImputedPrice'
    medianImputedPrice_column = 'medianImputedPrice'
    unknownImputedTitle_column = 'unknownImputedTitle'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------

    product_data = product_data.withColumn('price', product_data.price.cast('float'))
    product_data = product_data.drop('asin', 'salesRank','categories', 'related')
    product_data = product_data.withColumn('medianImputedPrice',  product_data.price)
    product_data = product_data.withColumn('meanImputedPrice', product_data.price)
    product_data = product_data.withColumn('unknownImputedTitle', product_data.title)    
    mean_data = (product_data.agg(F.avg('price')).collect()[0][0])
    product_data = product_data.fillna(mean_data, subset=['meanImputedPrice'])
        
    median_data = product_data.approxQuantile('price', [0.5], 0.01)[0]
    product_data = product_data.fillna(median_data, subset=['medianImputedPrice'])
    
    def clean_title(x):
        if x:
            return str(x)
        else:
            return 'unknown'
            
    clean_title_udf = udf(clean_title, StringType())

    product_data = product_data.withColumn('unknownImputedTitle', clean_title_udf('title'))

    count_total = product_data.count()

    mean_meanImputed = product_data.select(F.avg('meanImputedPrice')).collect()[0][0]
    variance_meanImputed = product_data.select(F.variance('meanImputedPrice')).collect()[0][0]
    numNulls_meanImputed = product_data.filter(F.col('meanImputedPrice').isNull()).count()
    
    mean_median = product_data.select(F.avg('medianImputedPrice')).collect()[0][0]
    variance_median = product_data.select(F.variance('medianImputedPrice')).collect()[0][0]
    numNulls_median = product_data.filter(F.col('medianImputedPrice').isNull()).count()
    numUnknowns = product_data.filter(F.col('unknownImputedTitle') == 'unknown').count()




    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_meanImputedPrice': None,
        'variance_meanImputedPrice': None,
        'numNulls_meanImputedPrice': None,
        'mean_medianImputedPrice': None,
        'variance_medianImputedPrice': None,
        'numNulls_medianImputedPrice': None,
        'numUnknowns_unknownImputedTitle': None
    }
    # Modify res:
    
    res['count_total'] = int(count_total)
    res['mean_meanImputedPrice'] = float(mean_meanImputed) if float(mean_meanImputed) else None
    res['variance_meanImputedPrice'] = float(variance_meanImputed)
    res['numNulls_meanImputedPrice'] = int(numNulls_meanImputed)
    res['mean_medianImputedPrice'] = float(mean_median) if float(mean_median) else None
    res['variance_medianImputedPrice'] = float(variance_median) 
    res['numNulls_medianImputedPrice'] = int(numNulls_median) 
    res['numUnknowns_unknownImputedTitle'] = float(numUnknowns)



    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_4')
    return res
    # -------------------------------------------------------------------------


def task_5(data_io, product_processed_data, word_0, word_1, word_2):
    # -----------------------------Column names--------------------------------
    # Inputs:
    title_column = 'title'
    # Outputs:
    titleArray_column = 'titleArray'
    titleVector_column = 'titleVector'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
        
            
    clean_title_udf = udf(clean_title, ArrayType(StringType()))
    product_processed_data = product_processed_data.withColumn('titleArray', clean_title_udf('title'))
    product_processed_data_output = product_processed_data.select('titleArray')

    word2vec = Word2Vec()
    word2vec.setInputCol('titleArray')  
    word2vec.setOutputCol('titleVector')
    word2vec.setMinCount(100)
    word2vec.setVectorSize(16)
    word2vec.setNumPartitions(4)
    word2vec.setSeed(SEED)
    
    model = word2vec.fit(product_processed_data_output)




    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'size_vocabulary': None,
        'word_0_synonyms': [(None, None), ],
        'word_1_synonyms': [(None, None), ],
        'word_2_synonyms': [(None, None), ]
    }
    # Modify res:
    res['count_total'] = product_processed_data_output.count()
    res['size_vocabulary'] = model.getVectors().count()
    for name, word in zip(
        ['word_0_synonyms', 'word_1_synonyms', 'word_2_synonyms'],
        [word_0, word_1, word_2]
    ):
        res[name] = model.findSynonymsArray(word, 10)



    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_5')
    return res
    # -------------------------------------------------------------------------


def task_6(data_io, product_processed_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    category_column = 'category'
    # Outputs:
    categoryIndex_column = 'categoryIndex'
    categoryOneHot_column = 'categoryOneHot'
    categoryPCA_column = 'categoryPCA'
    # -------------------------------------------------------------------------    

    # ---------------------- Your implementation begins------------------------
    
    indexer = M.feature.StringIndexer(
        inputCol=category_column,
        outputCol=categoryIndex_column
    )
    
    indexer_fitted = indexer.fit(product_processed_data)
    data_with_index = indexer_fitted.transform(product_processed_data)
    
    encoder = M.feature.OneHotEncoder(
        inputCol=categoryIndex_column,      
        outputCol=categoryOneHot_column,    
        dropLast=False                      
    )

    encoder_fitted = encoder.fit(data_with_index)
    data_with_onehot = encoder_fitted.transform(data_with_index)
    
    pca = M.feature.PCA(
        inputCol=categoryOneHot_column,  
        outputCol=categoryPCA_column,    
        k=15                             
    )
    
    pca_fitted = pca.fit(data_with_onehot)
    final_data = pca_fitted.transform(data_with_onehot)
    

    mean_onehot = final_data.select(
        Summarizer.mean(F.col(categoryOneHot_column)).alias("mean")
    ).collect()[0]["mean"]

    mean_pca = final_data.select(
        Summarizer.mean(F.col(categoryPCA_column)).alias("mean")
    ).collect()[0]["mean"]

    mean_onehot_list = mean_onehot.toArray().tolist()
    mean_pca_list = mean_pca.toArray().tolist()




    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'meanVector_categoryOneHot': [None, ],
        'meanVector_categoryPCA': [None, ]
    }
    # Modify res:

    res = {
        'count_total': final_data.count(),
        'meanVector_categoryOneHot': mean_onehot_list,
        'meanVector_categoryPCA': mean_pca_list         
    }


    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_6')
    return res
    # -------------------------------------------------------------------------
    
    
def task_7(data_io, train_data, test_data):
    
    # ---------------------- Your implementation begins------------------------
    
    tree = M.regression.DecisionTreeRegressor(featuresCol = 'features', 
                                              labelCol = 'overall',
                                              predictionCol = "prediction",
                                             maxDepth = 5).fit(train_data)
    
    predictions = tree.transform(test_data)
    
    
    
    # -------------------------------------------------------------------------
    
    
    # ---------------------- Put results in res dict --------------------------
    res = {
        'test_rmse': None
    }
    # Modify res:

    res['test_rmse'] = M.evaluation.RegressionEvaluator(
        labelCol = 'overall',
        predictionCol = 'prediction',
        metricName = 'rmse'
    ).evaluate(predictions)
    
    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_7')
    return res
    # -------------------------------------------------------------------------
    
    
def task_8(data_io, train_data, test_data):
    
    # ---------------------- Your implementation begins------------------------
    
    
    new_train, test_set = train_data.randomSplit([0.75, 0.25], seed=123)

    depths = [5, 7, 9, 12]
    RMSE_val = {}
    best_model = None
    best_rmse = float('inf')
    evaluator = RegressionEvaluator(labelCol="overall", predictionCol="prediction", metricName="rmse")

    for depth in depths:
        dt = DecisionTreeRegressor(featuresCol="features", labelCol="overall", maxDepth=depth)
        model = dt.fit(new_train)
        predictions = model.transform(test_set)
        rmse = evaluator.evaluate(predictions)
        RMSE_val[depth] = rmse

        if rmse < best_rmse:
            best_rmse = rmse
            best_model = model
    
    
    # -------------------------------------------------------------------------
    
    
    # ---------------------- Put results in res dict --------------------------
    res = {
        'test_rmse': None,
        'valid_rmse_depth_5': None,
        'valid_rmse_depth_7': None,
        'valid_rmse_depth_9': None,
        'valid_rmse_depth_12': None,
    }
    # Modify res:

    res['test_rmse'] = float(evaluator.evaluate(best_model.transform(test_data)))
    res['valid_rmse_depth_5'] = float(RMSE_val[5])
    res['valid_rmse_depth_7'] = float(RMSE_val[7])
    res['valid_rmse_depth_9'] = float(RMSE_val[9])
    res['valid_rmse_depth_12'] = float(RMSE_val[12])

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_8')
    return res
    # -------------------------------------------------------------------------

