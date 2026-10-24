import os
import psycopg2
from psycopg2 import sql
import multiprocessing
import numpy as np

os.environ.setdefault("PGDATABASE", "ernieplus")

def select_rows_with_range(start_index, end_index):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect("")  # Replace with your connection parameters
        cur = conn.cursor()

        # Execute the SELECT query to obtain rows within the specified range
        # cur.execute("SELECT * FROM hm31.in_edges_features_cert  ORDER BY id OFFSET %s LIMIT %s", (start_index, end_index - start_index))
        cur.execute("SELECT * FROM hm31.embedding_edge_features WHERE id >= %s AND id < %s", (start_index, end_index))

        # Fetch the results_temp
        results = cur.fetchall()

        # Print the results_temp
        #         for row in results_temp:
        #             print("Row:", type(row))

        return results

    except Exception as e:
        print("Error:", e)
        print(f"start {start_index} end {end_index}")

    finally:
        cur.close()
        conn.close()


def process_embedding(row):
    first_emb = row[3]
    second_emb = row[4]

    if type(first_emb) == list and len(first_emb) > 100:
        f = np.asarray(first_emb)

        if type(second_emb) == list and len(second_emb) > 100:
            s = np.asarray(second_emb)
            return np.dot(f,s)/(np.linalg.norm(f)*np.linalg.norm(s))

    return 0.5

def insert_values_into_table(values_list):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect("")
        cur = conn.cursor()


        args = ','.join(cur.mogrify("(%s, %s, %s)", i).decode('utf-8') for i in values_list)

        cur.execute("INSERT INTO hm31.embedding_edge_weights_cert VALUES " + (args))

        conn.commit()
    #         print('success')

    except Exception as e:
        print("Error:", e)

    finally:
        cur.close()
        conn.close()


def process_batch(rows):
    results = []
    for row in rows:
        first_node = row[1]
        second_node = row[2]

        cosine = process_embedding(row)

        results.append((first_node, second_node, cosine))

    return results


def get_table_count():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect("")  # Replace with your connection parameters
        cur = conn.cursor()

        # Execute the SELECT COUNT(*) query
        cur.execute("SELECT COUNT(*) FROM hm31.embedding_edge_features")

        # Fetch the result
        result = cur.fetchone()

        # Print the result
        return result[0]

    except Exception as e:
        print("Error:", e)

    finally:
        cur.close()
        conn.close()


def insert_wrapper(start_index, end_index):
    rows = select_rows_with_range(start_index, end_index)
    results = process_batch(rows)
    insert_values_into_table(results)



if __name__ == '__main__': #81958
    count = int(get_table_count())
    lst = []
    print(count)
    SPLIT_LENGTH = 10000
    # SPLIT_LENGTH = 100

    CHUNCKS = count // SPLIT_LENGTH + 1

    for i in range(CHUNCKS):
        lst.append((i*SPLIT_LENGTH,min((i+1)* SPLIT_LENGTH, count + 1)))

    print(lst[0])
    print(lst[-1])

    # print(lst)

    with multiprocessing.Pool(processes=40) as pool:
        results = pool.starmap(insert_wrapper, lst)

    # from tqdm import tqdm
    #
    # SPLIT_LENGTH = 10000
    # CHUNCKS = count // SPLIT_LENGTH
    #
    # for i in tqdm(range(CHUNCKS)):
    #     print('a')
    #     rows = select_rows_with_range(i*SPLIT_LENGTH, min((i+1)* SPLIT_LENGTH, count))
    #     print('b')
    #     results_temp = process_batch(rows)
    #     print('c')
    #     insert_values_into_table(results_temp)
    #     print('d')
    #     print()
    #
    #     if i == 10:
    #         break

