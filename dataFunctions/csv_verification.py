from glob import glob as globlin ## The 7bb globlin
import pandas as pd

def get_csv_file_list(path):
    csv_list = globlin(path)
    if len(csv_list) >= 1:
        return True, len(csv_list), csv_list
    else:
        return False

def check_each_csv_count(path_list):
    for path in path_list:
        print(check_single_csv(path))

def check_single_csv(path):
    single_df = pd.read_csv(path, sep = ';')
    if len(single_df) < 1:
        return False
    else:
        return True, len(single_df)

if __name__ == '__main__':
    category = 'plant'
    _, total_csv_length, csv_list = get_csv_file_list(f'./final_data/{category}/*.*')
    check_each_csv_count(csv_list)
