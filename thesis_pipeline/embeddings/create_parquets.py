import multiprocessing

def handle_single_file(npy_adress, pmids_json_adress, file_number, save_address):
    embeddings = np.load(npy_adress)

    with open(pmids_json_adress, "r") as file:
        pmids = json.load(file)


    all_array = []



    for idx, pmid in enumerate(pmids):
        all_array.append({'pmid': int(pmid), 'embedding': embeddings[idx]})

    df = pd.DataFrame(all_array)
    df.set_index(['pmid'], inplace=True)


    df.to_parquet(f'{save_address}{file_number}.parquet')




def wrapper(downloaded_files_dir, save_address, cores):
    files = os.listdir(downloaded_files_dir)

    files_dict = {}

    for file in files:
        file_number = int(file.split('.')[0].split('_')[-1])

        if not file_number in files_dict:
            files_dict[file_number] = {}

        if 'npy' in file:
            files_dict[file_number]['embedding'] = file

        else:
            files_dict[file_number]['pmids'] = file


    lst = []
    #def handle_single_file(npy_adress, pmids_json_adress, file_number, save_address):

    for file_number, pair_dict in files_dict.items():
        lst.append((downloaded_files_dir+pair_dict['embedding'], downloaded_files_dir+pair_dict['pmids'], file_number, save_address))


    with multiprocessing.Pool(processes=cores) as pool:
        pool.starmap(handle_single_file, lst)














if __name__ == '__main__':
    import time
    import multiprocessing
    import os
    import numpy as np
    import xmltodict
    import pandas as pd
    import argparse
    import requests
    import json
    import re
    import pandas as pd


    #files = get_all_file_names(base_url)


    #download_wrapper(files, save_dir, base_url, 70)





    downloaded_files_dir = '/shared/hossein_hm31/embeddings_downloaded/'
    save_address = '/shared/hossein_hm31/embeddings_parquets/'

    wrapper(downloaded_files_dir, save_address, 10)
