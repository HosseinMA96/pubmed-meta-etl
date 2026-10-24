import os
#Switch from sql to local as a parquet file
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.driver.extraClassPath", "postgresql-42.5.2.jar").config("spark.executor.extraClassPath","postgresql-42.5.2.jar") \
        .config("spark.local.dir", "./logs/") \
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


    normalized = spark.read.parquet('normalized/')
    normalized.cache()
    normalized.count()
    # normalized = normalized.limit(200)
    normalized = normalized.orderBy("id")

    # last_20_rows = normalized.tail(20)
    #
    # # Display the result
    # for row in last_20_rows:
    #     print(row)
    #
    # exit(0)
    #I did this to add index for reproducibility
    # normalized_with_id = normalized.withColumn("id", monotonically_increasing_id())
    #
    # # Order the DataFrame based on the columns 'first' and 'second'
    # normalized_with_id = normalized_with_id.orderBy("id")
    # normalized_with_id.show()
    #
    # normalized_with_id.coalesce(1).write.parquet('normalized/')
    #
    #

    coefficients = [1, 2, 3, 4, 5, 6, 7]
    scale = 1

    linear_coefficients = []
    sum_all = sum(coefficients)

    for coefficient in coefficients:
        linear_coefficients.append(coefficient/sum_all)

    columns_to_aggregate = ['year_similarity', 'mesh_median', 'cocitation_jaccard', 'cocitation_frequency', 'bib_jaccard', 'bib_frequency', 'cosine_similarity']
    normalized = normalized.withColumn("weight", sum(col(col_name) * coeff * scale for col_name, coeff in zip(columns_to_aggregate, linear_coefficients)))

    normalized = normalized.drop(*columns_to_aggregate)
    normalized.show()

    coefficients.append(scale)
    coefficients = list(map(str, coefficients))






    file_str = f'./parquets/edge_features_parquet_{"-".join(coefficients)}/'
    normalized.coalesce(1).write.parquet(file_str)
    print(normalized.count())

