import os
#Switch from sql to local as a parquet file
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
import statistics
import pandas
import ast
import json

def read_df(spark, table_name, jdbc_properties ):
    # df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
    # df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties, numPartitions=20, fetchsize=10000)

    df = spark.read.format('jdbc').options(
        url=jdbc_properties['jdbc_url'],
        #driver="org.postgresql.Driver",
        user=jdbc_properties["user"],
        password=jdbc_properties["password"],
        dbtable=table_name,
        fetchsize= 10000,
        numPartitions = 20
    ).load()
    return df


if __name__ == '__main__':

    import time
    num_part = 500

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.driver.extraClassPath", "postgresql-42.5.2.jar").config("spark.executor.extraClassPath","postgresql-42.5.2.jar") \
        .config("spark.local.dir", "/shared/hm31") \
        .config("spark.master", "local[*]") \
        .getOrCreate()

    jdbc_url = os.environ["CGF_JDBC_URL"]

    jdbc_properties = {
        "user": os.environ["CGF_DB_USER"],
        "password": os.environ["CGF_DB_PASSWORD"],
        "driver": "org.postgresql.Driver",
        'jdbc_url' : os.environ["CGF_JDBC_URL"]
    }


    spark.sparkContext.setLogLevel("WARN")


    all_edge_features = read_df(spark, 'hm31.all_edge_features_cert', jdbc_properties)



    #drop mesh_median
    all_edge_features = all_edge_features.drop('mesh_similarity_mean')
    # all_edge_features.write.parquet('/shared/hossein_hm31/edge_features_parquet/')


    #Replace null with zero
    all_edge_features = all_edge_features.fillna(0)

    #|  first| second|year_similarity|mesh_similarity_median|cocitation_jaccard_similarity|cocitation_frequency_similarity|bib_coupling_jaccard_similarity|bib_coupling_frequency_similarity|cosine_similarity|

    # all_edge_features = all_edge_features.limit(100)
    # all_edge_features.count()


    columns_to_scale = ["mesh_similarity_median", "cocitation_jaccard_similarity", "cocitation_frequency_similarity", 'bib_coupling_jaccard_similarity', 'bib_coupling_frequency_similarity']
    # columns_to_scale = ["mesh_similarity_median"]

    new_column_name = ['mesh_median', 'cocitation_jaccard', 'cocitation_frequency', 'bib_jaccard', 'bib_frequency']

    for idx, column in enumerate(columns_to_scale):
        all_edge_features = all_edge_features.repartition(num_part)

        print(f'index: {idx} ')
        assembler = VectorAssembler(inputCols=[column], outputCol=column+'_vec')
        scaler = MinMaxScaler(inputCol=column+'_vec', outputCol=column + '_scaled')
        pipeline = Pipeline(stages=[assembler, scaler])
        scalerModel = pipeline.fit(all_edge_features)
        all_edge_features = scalerModel.transform(all_edge_features)

        all_edge_features = all_edge_features.drop(column)
        all_edge_features = all_edge_features.drop(column+'_vec')

        firstelement=F.udf(lambda v:float(v[0]),FloatType())
        all_edge_features = all_edge_features.withColumn(new_column_name[idx], firstelement(column + '_scaled'))
        all_edge_features = all_edge_features.drop(column+'_scaled')

        all_edge_features = all_edge_features.repartition(1)
        all_edge_features.cache()
        temp = all_edge_features.count()


        all_edge_features.cache()
        temp = all_edge_features.count()

    # start = time.time()
    #
    # all_edge_features = all_edge_features.repartition(num_part)

    # column = columns_to_scale[0]
    # new_column = new_column_name[0]
    #
    # print(f'index: {0} ')
    # assembler = VectorAssembler(inputCols=[column], outputCol=column+'_vec')
    # scaler = MinMaxScaler(inputCol=column+'_vec', outputCol=column + '_scaled')
    # pipeline = Pipeline(stages=[assembler, scaler])
    # scalerModel = pipeline.fit(all_edge_features)
    # all_edge_features = scalerModel.transform(all_edge_features)
    #
    # all_edge_features = all_edge_features.drop(column)
    # all_edge_features = all_edge_features.drop(column+'_scaled')
    #
    # firstelement=F.udf(lambda v:float(v[0]),FloatType())
    # all_edge_features = all_edge_features.withColumn(new_column, firstelement(column + '_vec'))
    # all_edge_features = all_edge_features.drop(column+'_vec')
    #
    # all_edge_features = all_edge_features.repartition(1)
    #
    # all_edge_features.cache()
    # temp = all_edge_features.count()
    #
    # end = time.time()

    #
    # all_edge_features = all_edge_features.repartition(100)
    #
    # column = columns_to_scale[1]
    # new_column = new_column_name[1]
    #
    # print(f'index: {1} ')
    # assembler = VectorAssembler(inputCols=[column], outputCol=column+'_vec')
    # scaler = MinMaxScaler(inputCol=column+'_vec', outputCol=column + '_scaled')
    # pipeline = Pipeline(stages=[assembler, scaler])
    # scalerModel = pipeline.fit(all_edge_features)
    # all_edge_features = scalerModel.transform(all_edge_features)
    #
    # all_edge_features = all_edge_features.drop(column)
    # all_edge_features = all_edge_features.drop(column+'_scaled')
    #
    # firstelement=F.udf(lambda v:float(v[0]),FloatType())
    # all_edge_features = all_edge_features.withColumn(new_column, firstelement(column + '_vec'))
    # all_edge_features = all_edge_features.drop(column+'_vec')
    #
    # all_edge_features.cache()
    # temp = all_edge_features.count()
    #
    #
    #
    #
    # all_edge_features = all_edge_features.repartition(100)
    #
    # column = columns_to_scale[2]
    # new_column = new_column_name[2]
    #
    # print(f'index: {2} ')
    # assembler = VectorAssembler(inputCols=[column], outputCol=column+'_vec')
    # scaler = MinMaxScaler(inputCol=column+'_vec', outputCol=column + '_scaled')
    # pipeline = Pipeline(stages=[assembler, scaler])
    # scalerModel = pipeline.fit(all_edge_features)
    # all_edge_features = scalerModel.transform(all_edge_features)
    #
    # all_edge_features = all_edge_features.drop(column)
    # all_edge_features = all_edge_features.drop(column+'_scaled')
    #
    # firstelement=F.udf(lambda v:float(v[0]),FloatType())
    # all_edge_features = all_edge_features.withColumn(new_column, firstelement(column + '_vec'))
    # all_edge_features = all_edge_features.drop(column+'_vec')
    #
    # all_edge_features.cache()
    # temp = all_edge_features.count()
    #
    #
    #
    # all_edge_features = all_edge_features.repartition(100)
    #
    # column = columns_to_scale[3]
    # new_column = new_column_name[3]
    #
    # print(f'index: {3} ')
    # assembler = VectorAssembler(inputCols=[column], outputCol=column+'_vec')
    # scaler = MinMaxScaler(inputCol=column+'_vec', outputCol=column + '_scaled')
    # pipeline = Pipeline(stages=[assembler, scaler])
    # scalerModel = pipeline.fit(all_edge_features)
    # all_edge_features = scalerModel.transform(all_edge_features)
    #
    # all_edge_features = all_edge_features.drop(column)
    # all_edge_features = all_edge_features.drop(column+'_scaled')
    #
    # firstelement=F.udf(lambda v:float(v[0]),FloatType())
    # all_edge_features = all_edge_features.withColumn(new_column, firstelement(column + '_vec'))
    # all_edge_features = all_edge_features.drop(column+'_vec')
    #
    # all_edge_features.cache()
    # temp = all_edge_features.count()
    #
    #
    #
    # all_edge_features = all_edge_features.repartition(100)
    #
    # column = columns_to_scale[4]
    # new_column = new_column_name[4]
    #
    # print(f'index: {4} ')
    # assembler = VectorAssembler(inputCols=[column], outputCol=column+'_vec')
    # scaler = MinMaxScaler(inputCol=column+'_vec', outputCol=column + '_scaled')
    # pipeline = Pipeline(stages=[assembler, scaler])
    # scalerModel = pipeline.fit(all_edge_features)
    # all_edge_features = scalerModel.transform(all_edge_features)
    #
    # all_edge_features = all_edge_features.drop(column)
    # all_edge_features = all_edge_features.drop(column+'_scaled')
    #
    # firstelement=F.udf(lambda v:float(v[0]),FloatType())
    # all_edge_features = all_edge_features.withColumn(new_column, firstelement(column + '_vec'))
    # all_edge_features = all_edge_features.drop(column+'_vec')
    #
    # all_edge_features.cache()
    # temp = all_edge_features.count()
    #
    #
    #
    #
    #
    #
    #
    #





    # all_edge_features.printSchema()



    #elapsed 483.2492923736572 part 500


    # elapsed = end - start


    # assemblers = [VectorAssembler(inputCols=[col], outputCol=col + "_vec") for col in columns_to_scale]
    # scalers = [MinMaxScaler(inputCol=col + "_vec", outputCol=col + "_scaled") for col in columns_to_scale]
    # pipeline = Pipeline(stages=assemblers + scalers)
    #
    # scalerModel = pipeline.fit(all_edge_features)
    # all_edge_features = scalerModel.transform(all_edge_features)

    # print(f'elapsed {elapsed} part {num_part}')

    all_edge_features.write.parquet('./normalized/')

    # `elapsed 498.75388288497925 part 250
    #
    print('all count', temp)
    # all_edge_features.show(100)

    all_edge_features.unpersist()
