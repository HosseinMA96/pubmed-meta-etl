import os
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
import statistics
import pandas
import ast
import json




#Read a df from db
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


def write_df(result, table_name, jdbc_properties):
    result = result.repartition(10)
    result.write.format('jdbc').options(
        url=jdbc_properties['jdbc_url'],
        driver="org.postgresql.Driver",
        user=jdbc_properties["user"],
        password=jdbc_properties["password"],
        dbtable=table_name,
        mode="overwrite"
    ).save()

def read_write_parquet(read_dir, table_name, jdbc_properties):
    df = spark.read.parquet(read_dir)
    df = df.repartition(50)
    write_df(df, table_name, jdbc_properties)





def calculate_year_feature(row):

    # Assuming 'first' and 'second' are node IDs
    first_year = int(row['first_year'])
    second_year = int(row['second_year'])

    year_diff = abs(first_year - second_year)

    if year_diff <= 5:
        return 1.0

    if year_diff <= 10:
        return 0.5

    return 0.25








def handle_year(year_mesh_edge_duplicate_features):


    calculate_year_difference_udf = F.udf(lambda row: calculate_year_feature(row), FloatType())
    #edge_years = edge_years.withColumn('year_feature', calculate_year_difference_udf(F.struct(edge_years['first_year'], edge_years['second_year'])))

    year_mesh_edge_duplicate_features = year_mesh_edge_duplicate_features.repartition(200)

    year_mesh_edge_duplicate_features = year_mesh_edge_duplicate_features.withColumn(
        'year_similarity',
        F.when(F.abs(year_mesh_edge_duplicate_features['first_year'] - year_mesh_edge_duplicate_features['second_year']) <= 5, 1.0)
        .when((5 < F.abs(year_mesh_edge_duplicate_features['first_year'] - year_mesh_edge_duplicate_features['second_year'])) & (F.abs(year_mesh_edge_duplicate_features['first_year'] - year_mesh_edge_duplicate_features['second_year']) <= 10), 0.5)
        .otherwise(0.25)
    )

    columns_to_drop = ['first_year', 'second_year']
    year_mesh_edge_duplicate_features = year_mesh_edge_duplicate_features.drop(*columns_to_drop)

    year_mesh_edge_duplicate_features = year_mesh_edge_duplicate_features.repartition(1)
    year_mesh_edge_duplicate_features.cache()


    print(f'year completed count {year_mesh_edge_duplicate_features.count()}')

    return year_mesh_edge_duplicate_features

#Calculate mesh_pair_similarity by counting common terms between two mesh strings
#Here we assume meshes are of the form D23.300.820', 'D23.550.291.125
#We consider similarity for the letter as well (D vs D)
def calculate_mesh_pair_similarity(mesh1, mesh2):
    global max_mesh_overlap

    mesh1 = mesh1[0] + '.' + mesh1[1:]
    mesh2 = mesh2[0] + '.' + mesh2[1:]

    mesh_1_decomp = mesh1.split('.')
    mesh_2_decomp = mesh2.split('.')


    common_terms = 0
    limit = min(len(mesh_1_decomp), len(mesh_2_decomp))
    for i in range(limit):
        if mesh_1_decomp[i] == mesh_2_decomp[i]:
            common_terms += 1

        else:
            break

    return common_terms



with open('mesh.json', 'r') as json_file:
    mesh_dict = json.load(json_file)


def calculate_mesh_similarity_mean(row, mesh_lookup = mesh_dict):

    similarities = []
    mesh_str_1 = row['first_mesh']
    mesh_str_2 = row['second_mesh']

    mesh_terms_1 = ast.literal_eval(mesh_str_1)
    mesh_terms_2 = ast.literal_eval(mesh_str_2)

    for first in mesh_terms_1:
        if not first in mesh_lookup:
            continue

        first_mesh_tree = mesh_lookup[first][0]
        for second in mesh_terms_2:
            if not second in mesh_lookup:
                continue

            second_mesh_tree = mesh_lookup[second][0]
            similarities.append(calculate_mesh_pair_similarity(first_mesh_tree, second_mesh_tree))
            # counter += 1


    if len(similarities) == 0:
        return 0.0

    avg = statistics.mean(similarities)
    return float(avg)


def calculate_mesh_similarity_median(row, mesh_lookup = mesh_dict):

    similarities = []
    mesh_str_1 = row['first_mesh']
    mesh_str_2 = row['second_mesh']

    mesh_terms_1 = ast.literal_eval(mesh_str_1)
    mesh_terms_2 = ast.literal_eval(mesh_str_2)

    counter = 0
    # return len(mesh_terms_1) + len(mesh_terms_2)
    #
    for first in mesh_terms_1:
        if not first in mesh_lookup:
            continue

        first_mesh_tree = mesh_lookup[first][0]
        for second in mesh_terms_2:
            if not second in mesh_lookup:
                continue

            second_mesh_tree = mesh_lookup[second][0]
            similarities.append(calculate_mesh_pair_similarity(first_mesh_tree, second_mesh_tree))
            # counter += 1

    # if len(similarities) == 0:
    #     return -1

    if len(similarities) == 0:
            return 0.0

    median = statistics.median(similarities)
    return float(median)








def handle_mesh(edges_annotated_with_year_features):

    edges_annotated_with_year_features = edges_annotated_with_year_features.repartition(200)

    calculate_mesh_similarity_udf = F.udf(lambda row: calculate_mesh_similarity_mean(row), FloatType())
    edges_annotated_with_year_features = edges_annotated_with_year_features.withColumn('mesh_similarity_mean', calculate_mesh_similarity_udf(F.struct(edges_annotated_with_year_features['first_mesh'], edges_annotated_with_year_features['second_mesh'])))

    calculate_mesh_similarity_udf = F.udf(lambda row: calculate_mesh_similarity_median(row), FloatType())
    edges_annotated_with_year_features = edges_annotated_with_year_features.withColumn('mesh_similarity_median', calculate_mesh_similarity_udf(F.struct(edges_annotated_with_year_features['first_mesh'], edges_annotated_with_year_features['second_mesh'])))

    columns_to_drop = ['first_mesh', 'second_mesh']
    edges_annotated_with_year_features = edges_annotated_with_year_features.drop(*columns_to_drop)

    edges_annotated_with_year_features.repartition(1)
    edges_annotated_with_year_features.cache()


    print(f'mesh completed count {edges_annotated_with_year_features.count()}')

    return edges_annotated_with_year_features







#Read edges, and for each features, calculate the similarities for that feature
def calculate_raw_node_similarities(spark):
    import time
    year_mesh_node_duplicated_edges = read_df(spark, jdbc_url, 'hm31.year_mesh_node_duplicated_edges_cert', jdbc_properties)
    year_mesh_node_duplicated_edges.persist()


    edges_annotated_with_year_features = handle_year(year_mesh_node_duplicated_edges)
    edges_annotated_with_year_and_mesh_features = handle_mesh(edges_annotated_with_year_features)


    edges_annotated_with_year_and_mesh_features.repartition(10).write.parquet('/shared/hossein_hm31/parquets_year_and_mesh')
    write_df(edges_annotated_with_year_and_mesh_features, 'hm31.year_mesh_edge_weights_cert', jdbc_properties)

    # def write_df(result, jdbc_url, table_name):

#If two arrays a1 and a2 are sorted, what is the size of their intersection?
def find_common_count(arr1, arr2):
    size1, size2 = len(arr1), len(arr2)
    i, j, count = 0, 0, 0

    while i < size1 and j < size2:
        if arr1[i] == arr2[j]:
            # Found an element in the intersection
            count += 1
            i += 1
            j += 1
        elif arr1[i] < arr2[j]:
            # Move the pointer in the first array
            i += 1
        else:
            # Move the pointer in the second array
            j += 1

    return count

#Calculate jaccard of bib_coupling of a row
def calculate_bib_coupling_jaccard_similarity(row):
    first_out = row['first_out']
    second_out = row['second_out']

    first_out_nodes = first_out.split(',')
    second_out_nodes = second_out.split(',')

    first_out_nodes.sort()
    second_out_nodes.sort()

    intersection_count = find_common_count(first_out_nodes, second_out_nodes)

    if len(first_out_nodes) + len(second_out_nodes) == 0:
        return 0.0

    return float(intersection_count/(len(first_out_nodes) + len(second_out_nodes) - intersection_count))

#Calculate jaccard of bib_coupling of a row
def calculate_bib_coupling_raw_frequency(row):
    first_out = row['first_out']
    second_out = row['second_out']

    first_out_nodes = first_out.split(',')
    second_out_nodes = second_out.split(',')

    first_out_nodes.sort()
    second_out_nodes.sort()

    intersection_count = find_common_count(first_out_nodes, second_out_nodes)

    return intersection_count

#####################################################################################3

#Calculate jaccard of bib_coupling of a row
def calculate_cocitation_jaccard_similarity(row):
    first_in = row['first_in']
    second_in = row['second_in']

    first_in_nodes = first_in.split(',')
    second_in_nodes = second_in.split(',')

    first_in_nodes.sort()
    second_in_nodes.sort()

    intersection_count = find_common_count(first_in_nodes, second_in_nodes)

    if len(first_in_nodes) + len(second_in_nodes) == 0:
        return 0.0

    return float(intersection_count/(len(first_in_nodes) + len(second_in_nodes) - intersection_count))

#Calculate jaccard of bib_coupling of a row
def calculate_cocitation_raw_similarity(row):
    first_in = row['first_in']
    second_in = row['second_in']

    first_in_nodes = first_in.split(',')
    second_in_nodes = second_in.split(',')

    first_in_nodes.sort()
    second_in_nodes.sort()

    intersection_count = find_common_count(first_in_nodes, second_in_nodes)

    return intersection_count


def handle_cocitation(spark):
    # citations_duplicated_edges = read_df(spark, jdbc_url, 'hm31.cocitation_limited', jdbc_properties)
    citations_duplicated_edges = read_df(spark, 'hm31.cocitation_limited', jdbc_properties)

    print("read from db")
    # citations_duplicated_edges = read_df(spark, jdbc_url, 'hm31.limited', jdbc_properties)
    # # references_duplicated_edges = references_duplicated_edges.limit(40)

    citations_duplicated_edges = citations_duplicated_edges.repartition(50)

    calculate_cocitation_jaccard_similarity_udf = F.udf(lambda row: calculate_cocitation_jaccard_similarity(row), FloatType())
    citations_duplicated_edges_plus_jaccard = citations_duplicated_edges.withColumn('cocitation_jaccard_similarity', calculate_cocitation_jaccard_similarity_udf(F.struct(citations_duplicated_edges['first_in'], citations_duplicated_edges['second_in'])))
    print('co_citation_jaccard_similarity completed')


    calculate_cocitation_raw_frequency_udf = F.udf(lambda row: calculate_cocitation_raw_similarity(row), IntegerType())
    citations_duplicated_edges_plus_jaccard_and_frequency = citations_duplicated_edges_plus_jaccard.withColumn('cocitation_frequency_similarity', calculate_cocitation_raw_frequency_udf(F.struct(citations_duplicated_edges_plus_jaccard['first_in'], citations_duplicated_edges_plus_jaccard['second_in'])))
    print('calculate_cocitation_raw_frequency completed')


    columns_to_drop = ['first_in', 'second_in']
    citations_duplicated_edges_plus_jaccard_and_frequency = citations_duplicated_edges_plus_jaccard_and_frequency.drop(*columns_to_drop)

    print('dropped')
    citations_duplicated_edges_plus_jaccard_and_frequency = citations_duplicated_edges_plus_jaccard_and_frequency.repartition(1)
    # citations_duplicated_edges_plus_jaccard_and_frequency.cache()
    print('count', citations_duplicated_edges_plus_jaccard_and_frequency.count())

    print('cocitations completed')
    return citations_duplicated_edges_plus_jaccard_and_frequency




def handle_bib_coupling(spark):
    references_duplicated_edges = read_df(spark, jdbc_url, 'hm31.out_edges_features_cert', jdbc_properties)
    print("read from db")
    # references_duplicated_edges = read_df(spark, jdbc_url, 'hm31.limited', jdbc_properties)

    references_duplicated_edges = references_duplicated_edges.repartition(200)

    calculate_bib_coupling_jaccard_similarity_udf = F.udf(lambda row: calculate_bib_coupling_jaccard_similarity(row), FloatType())
    references_duplicated_edges_plus_jaccard = references_duplicated_edges.withColumn('bib_coupling_jaccard_similarity', calculate_bib_coupling_jaccard_similarity_udf(F.struct(references_duplicated_edges['first_out'], references_duplicated_edges['second_out'])))
    print('bib_coupling_jaccard_similarity completed')


    calculate_bib_coupling_raw_frequency_udf = F.udf(lambda row: calculate_bib_coupling_raw_frequency(row), IntegerType())
    references_duplicated_edges_plus_jaccard_and_frequency = references_duplicated_edges_plus_jaccard.withColumn('bib_coupling_frequency_similarity', calculate_bib_coupling_raw_frequency_udf(F.struct(references_duplicated_edges_plus_jaccard['first_out'], references_duplicated_edges_plus_jaccard['second_out'])))
    print('bib_coupling_frequency_similarity completed')


    columns_to_drop = ['first_out', 'second_out']
    references_duplicated_edges_plus_jaccard_and_frequency = references_duplicated_edges_plus_jaccard_and_frequency.drop(*columns_to_drop)

    # edges_annotated_with_year_features.show()
    references_duplicated_edges_plus_jaccard_and_frequency = references_duplicated_edges_plus_jaccard_and_frequency.repartition(1)
    references_duplicated_edges_plus_jaccard_and_frequency.cache()
    print('count', references_duplicated_edges_plus_jaccard_and_frequency.count())


    print('bib-coupling completed')
    return references_duplicated_edges_plus_jaccard_and_frequency


def wrap_bib_coupling(spark):
    references_duplicated_edges_plus_jaccard_and_frequency = handle_bib_coupling(spark)
    start = time.time()

    #cocitations_duplicated_edges_plus_jaccard_and_frequency.explain()
    references_duplicated_edges_plus_jaccard_and_frequency = references_duplicated_edges_plus_jaccard_and_frequency.repartition(1)
    references_duplicated_edges_plus_jaccard_and_frequency.repartition(10).write.parquet('/shared/parquets_bib/')
    write_df(references_duplicated_edges_plus_jaccard_and_frequency, 'hm31.bib_coupling_edge_weights_cert', jdbc_properties)


    end = time.time()
    print(f'elapsed time {end - start}')

def wrap_cocitation(spark): #156164
    cocitations_duplicated_edges_plus_jaccard_and_frequency = handle_cocitation(spark)
    start = time.time()
    cocitations_duplicated_edges_plus_jaccard_and_frequency.repartition(1).write.parquet('/shared/hossein_hm31/parquets_cocitation/')

    # write_df(cocitations_duplicated_edges_plus_jaccard_and_frequency, 'hm31_cocitation_edge_weights_cert', jdbc_properties)
    #write_df(cocitations_duplicated_edges_plus_jaccard_and_frequency, 'hm31.cocitation_edge_weights_cert', jdbc_properties)

    mid = time.time()
    print(f"parquet wrote elapsed {mid - start}")






#Read edges, and for each features, calculate the similarities for that feature
def calculate_raw_edge_similarities(spark):

    # wrap_bib_coupling(spark)
    wrap_cocitation(spark)











#This file obtains a table
if __name__ == "__main__":
    import time

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

    try:
        # calculate_raw_node_similarities(spark)
        calculate_raw_edge_similarities(spark)
        # read_write_parquet('/shared/parquets_cocitation','hm31.coooocccc', jdbc_properties)
        spark.stop()
        exit(0)

    except Exception as e:

        print(f'Error: {e}')
        spark.stop()
        exit(0)






"""
comments:
1st: repartition 200 for calculations
2nd: do calculations
3rd: cache
4th: count
5th: Write
"""



# 22323
