import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
import statistics
import pandas
import ast
import json

def read_df(spark, jdbc_url, table_name, jdbc_properties ):
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)
    return df

def read_write_parquet(read_dir, table_name, jdbc_properties):
    df = spark.read.parquet(read_dir)
    df = df.repartition(50)
    write_df(df, table_name, jdbc_properties)

def write_df(result, table_name, jdbc_properties):
    result = result.repartition(10)
    result.write.format('jdbc').options(
        url=jdbc_properties['jdbc_url'],
        driver="org.postgresql.Driver",
        user=jdbc_properties["user"],
        password=jdbc_properties["password"],
        dbtable=table_name,
        mode="append"
    ).save()


if __name__ == '__main__':
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
        "driver": "org.postgresql.Driver"
    }

    jdbc_properties = {
        "user": os.environ["CGF_DB_USER"],
        "password": os.environ["CGF_DB_PASSWORD"],
        "driver": "org.postgresql.Driver",
        'jdbc_url' : os.environ["CGF_JDBC_URL"]
    }



    spark.sparkContext.setLogLevel("WARN")

    parquet_path = '/shared/hossein_hm31/embeddings_test/'

    try:

        unique_nodes = read_df(spark, jdbc_url, 'hm31.unique_nodes_cert', jdbc_properties)
        df = spark.read.parquet(parquet_path)


        unique_nodes.createOrReplaceTempView("unique_nodes")
        df.createOrReplaceTempView("all_embeddings")

        sql_query = """
            select u.node_id, a.embedding from unique_nodes u inner join all_embeddings a on u.pmid = a.pmid
        """
        result =  spark.sql(sql_query)



        print("num", result.count())
        #result.repartition(10).write.parquet('/shared/parquets_embeddings/')
        # result.repartition(10).write.parquet('/shared/parquets_embeddings_2/')

        write_df(result, 'hm31.uncleaned_embeddings_cert_4', jdbc_properties)
        spark.stop()
        exit(0)





    except Exception as e:

        print(f'Error: {e}')
        spark.stop()
        exit(0)



    spark.stop()
    exit(0)