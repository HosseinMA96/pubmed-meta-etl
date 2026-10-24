import time

from pyspark.sql import SparkSession
import pandas as pd
import tables
import igraph as ig
import  leidenalg as la
import numpy as np
import networkx as nx
#Convert and save
def convert_to_pandas(spark, name ):
    df = spark.read.parquet('files/edges/')

    print(df.count())

    t1 = time.time()
    pandas_df = df.toPandas()
    t2 = time.time()
    pandas_df.to_hdf(name , key='data', mode='w')
    t3 = time.time()

    print(f'pyspark to pandas {t2-t1} pandas saved {t3-t2}')
    # pandas_df = pd.read_hdf('edges.h5', key='data')


def construct_graph(pandas_df):
    t1 = time.time()
    G = nx.from_pandas_edgelist(pandas_df,'first','second')
    t2 = time.time()
    # print(G.edges)

    G, mapping_df = relabel_networkx_nodes(G)
    t3= time.time()
    H = ig.Graph.from_networkx(G)
    t4=time.time()



    print(f'nx from pd {t2-t1} relabling {t3-t2} ig from nx {t4-t3}')

    return H, G, mapping_df


#Squeeze the graph ids into continuous one, as Igraph expects, and return a mapping dataframe
def relabel_networkx_nodes(G_networkx):
    nodes = G_networkx.nodes
    mapping_list = []
    mapping_dict = {}

    for i, node_id in enumerate(nodes):
        mapping_list.append({'original_id': node_id, 'squashed_id': i})
        mapping_dict[node_id] = i

    G_networkx_relabled = nx.relabel_nodes(G_networkx, mapping_dict)
    #Used later for metadata retrieval
    mapping_df =  pd.DataFrame(mapping_list)


    return G_networkx_relabled, mapping_df


def convert(array_mapping):
    modularity_format = {}
    for i in range(len(array_mapping)):
        if not array_mapping[i] in modularity_format:
            modularity_format[array_mapping[i]] = [i]
        else:
            modularity_format[array_mapping[i]].append(i)

    communities = []
    for _, value in modularity_format.items():
        communities.append(value)

    return communities

def read_pandas_edge_list(name):
    df = pd.read_hdf(name, key='data')
    return df


if __name__ == '__main__':

    spark = SparkSession.builder \
    .appName("parquet_clustering") \
    .config("spark.executor.memory", "10g") \
    .config("spark.driver.maxResultSize", "4g").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    name = 'files/pandas/edges_subset.h5'

    convert_to_pandas(spark, name)

    t1 = time.time()
    pandas = read_pandas_edge_list(name)
    t2 = time.time()

    print(f'read pandas from hdfs {t2-t1}')


    H_igraph, G_networkx, mapping_df = construct_graph(pandas)


    csv_file_path = f'files/graphs/mapping.csv'
    mapping_df.to_csv(csv_file_path, index=False)

    t1 = time.time()

    nx_path = f"files/graphs/base_nx.gpickle"
    ig_path = f"files/graphs/base_ig.pickle"
    #Save graph
    nx.write_gpickle(G_networkx, nx_path)
    ig.write(H_igraph, ig_path)

    t2 = time.time()

    print(f'saved elapsed {t2-t1}')
    #
    # for edge in G_networkx.edges():
    #     u, v = edge
    #     edge_data = G_networkx.get_edge_data(u, v)
    #     print(f"Edge {u} - {v}: {edge_data}")


    # print(H_igraph.get_edgelist())


    G_networkx = nx.read_gpickle(nx_path)
    H_igraph = ig.read(ig_path)
    t1 = time.time()

    print(f'load elapsed {t1-t2}')

    # print(H_igraph.get_edgelist())

    # num_nodes = H_igraph.vcount()
    # num_edges = H_igraph.ecount()
    #
    # print("Number of Nodes:", num_nodes)
    # print("Number of Edges:", num_edges)



    # x = leiden(H_igraph, 'Modularity', pandas)


    # t1 = time.time()
    # mod = nx.community.modularity(G_networkx, x)
    # t2 = time.time()
    #
    # print(f'modularity calculation {t2-t1}')
    # print(mod)

    #0.7242505159731305 w/o weights
    #0.0.7288022212244317 with weights

    #nx from pd 39.37786674499512 relabling 53.00694012641907 ig from nx 19.65127396583557  5000000







