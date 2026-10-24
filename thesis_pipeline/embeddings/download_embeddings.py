import multiprocessing

def download(file, save_address, base_url ):
    os.system(f'cd {save_address} && wget {base_url+file}')


#calculate all the filenames for embeddings and pmids to be downloaded
def get_all_file_names(url):
    response = requests.get(url)

    input_string = response.text

    pattern = r'<a href="([^"]+)">([^<]+)</a>'

    matches = re.findall(pattern, input_string)
    filenames = [match[1] for match in matches]
    files = [f for f in filenames if 'embeds' in f or 'pmids' in f]
    files.sort()

    return files

def download_wrapper(files, save_dir, base_url, cores):
    lst = []
    for file in files:
        lst.append((file, save_dir, base_url ))
    with multiprocessing.Pool(processes=cores) as pool:
        pool.starmap(download, lst)




if __name__ == '__main__':
    import time
    import multiprocessing
    import os
    import xmltodict
    import pandas as pd
    import argparse
    import requests
    import re

    base_url = 'https://ftp.ncbi.nlm.nih.gov/pub/lu/MedCPT/pubmed_embeddings/'
    save_dir = '/shared/hossein_hm31/embeddings/'
    files = get_all_file_names(base_url)


    #download_wrapper(files, save_dir, base_url, 70)



