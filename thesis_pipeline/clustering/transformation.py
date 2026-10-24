import argparse
import time
import pandas as pd
import sys
from pandarallel import pandarallel
def main():
    parser = argparse.ArgumentParser(description="Parse file name and function name.")
    parser.add_argument("file_name", help="Name of the file")
    parser.add_argument("function_name", help="Name of the function")
    # parser.add_argument("float_list", help="List of comma-separated floats (e.g., 1.0,2.0,3.0,4.0,5.0,6.0,7.0)")

    args = parser.parse_args()

    file_name = args.file_name
    function_name = args.function_name
    # float_list_str = args.float_list

    # Return the file name and function name
    #
    # if 'weight' in function_name:
    #     float_list = [float(x.strip()) for x in float_list_str.split(',')]
    #
    # else:
    #     float_list = []

    return file_name, function_name

def calculate_weight(row, weights, scale, shift):
    # Assuming weights is a list or array containing the coefficients for linear combination
    weight = scale*(row['year_similarity']* weights[0] + row['mesh_median']* weights[1]+
                    row['cocitation_jaccard']* weights[2] + row['cocitation_frequency']* weights[3] + row['bib_jaccard']* weights[4]
                    +  row['bib_frequency']* weights[5] + row['cosine_similarity']* weights[6] ) + shift


    return weight

def apply_feature_aggregation(df, weights, scale, shift, cores):
    t1 = time.time()
    #pandarallel.initialize(progress_bar=False) # initialize(36) or initialize(os.cpu_count()-1)
    pandarallel.initialize(nb_workers=cores, progress_bar=False) # initialize(36) or initialize(os.cpu_count()-1)
    new_df = df[['id']].copy()
    new_df['weight'] = df.parallel_apply(calculate_weight, axis=1, weights=weights, scale=scale, shift=shift)

    t2 = time.time()
    print(f'init weights {t2-t1}')

    return new_df

def apply_weight_scaling(df, cores):

    pandarallel.initialize(nb_workers=cores, progress_bar=False) # initialize(36) or initialize(os.cpu_count()-1)
    t2 = time.time()
    new_df, second_min, current_max = interval_mapping_preprocessing(df, 'weight')
    t3 = time.time()

    print(f'initial mapping preprocessing finished {t3-t2}')

    new_df['weight'] = new_df.parallel_apply(apply_linear_transformation, axis=1, second_min=second_min, current_max= current_max) #ERRRRRR

    # c_max = new_df['weight'].max()
    # c_min = new_df['weight'].min()

    t4 = time.time()

    print(f'I calculated second weights {t4-t3}')

    # print(new_df.head(3))
    #
    # print(c_min, c_max)
    print()

    return new_df


def apply_linear_transformation(row, second_min, current_max):
    new_max = 10
    new_min = 1

    weight = row['weight']
    new_weight = ((weight- second_min) * (new_max - new_min) / (current_max - second_min)) + new_min
    return new_weight



def interval_mapping_preprocessing(df, column_name):
    # Filter out zeros

    # Find the minimum and second minimum values
    current_min = df[column_name].min()
    second_min = df[df[column_name] > current_min][column_name].min()
    current_max = df[column_name].max()


    df.loc[df[column_name] == 0, column_name] = second_min
    #     print(df)

    return df, second_min, current_max
    # # Apply the transformation to non-zero values
    # df[column_name] = ((df[column_name] - second_min) * (new_max - new_min) / (current_max - second_min)) + new_min
    # return df

def load_list_from_txt(filename):
    with open(filename, 'r') as file:
        return [float(line.strip()) for line in file]

if __name__ == "__main__":
    # Call the main function to get the file name and function name
    file_name, function_name = main()


    # Use the file name and function name in the __main__ block
    print("File name:", file_name)
    print("Function name:", function_name)


    loaded_df = pd.read_hdf(file_name, key='data')
    print('loaded df')

    if 'weight' in function_name:
        print('inside weights')

        weights_txt = 'files/temp/weights.txt'
        weights = load_list_from_txt(weights_txt)

        print(f'retrieved weights {weights}')

        new_df = apply_feature_aggregation(df=loaded_df, weights=weights, scale=1, shift=0,cores=24)
        # print(new_df.head(10))
        print()

    elif 'scale' in function_name:
        print('scale')
        new_df= apply_weight_scaling(df=loaded_df, cores=24)

    save_dir = 'files/temp/temp_storage.h5'
    new_df.to_hdf(save_dir, key='data', mode='w')

    sys.exit(0)









