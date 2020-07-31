import numpy as np
import pandas as pd
from glob import glob as globlin ## The 7bb globlin

def get_img_features(main_path, category):
    feature_paths = globlin(main_path + f'/{category}/*.*')
    return feature_paths

def load_all_image_features(directories):
    feature_list = []
    for index, directory in enumerate(directories):
        column_name = str(directory.split('/')[-1].replace('.npz',''))
        print(f'Loading features for {column_name} -- {index}/{len(directories)}', end='\r')
        feature_list.append(pd.DataFrame(np.loadtxt(directory), columns = [column_name]))
    return pd.concat(feature_list, axis = 1)

def create_col_labels_for_features(feature_df):
    feature_df_columns = []
    number_of_features = len(feature_df)
    for idx in range(0, number_of_features):
        feature_df_columns.append('img_feature_' + str(idx+1))
    final_feature_df = feature_df.T
    final_feature_df.columns = feature_df_columns
    return final_feature_df

def add_ids_to_feature_df(feature_df, pic_ids):
    pic_column = pd.DataFrame(pic_ids, columns = ['picID'])
    pic_column.reset_index(drop=True, inplace=True)
    feature_df.reset_index(drop=True, inplace=True)
    return pd.concat([pic_column, feature_df], axis = 1)

def labeler(row):
    if 'plant' in row['picID']:
        return 0
    elif 'animal' in row['picID']:
        return 1
    elif 'human' in row['picID']:
        return 2

def divide_save_csv(df_length, image_df, category):
    iterations = int(df_length/10000)
    final_file_length = int(df_length%10000)
    row_count_beg = 0
    row_count_end = 10000
    for idx in range(0, iterations):
        print(f'Saving from index - {row_count_beg} to {row_count_end}', end = '\r')
        image_df.loc[row_count_beg:row_count_end].to_csv(f'./final_data/{category}/image_df_{idx+1}.csv', sep = ';')
        row_count_beg += 10000
        row_count_end += 10000
    print(f'Saving from index - {row_count_beg} to {final_file_length+row_count_end}', end = '\r')
    image_df.loc[row_count_beg: final_file_length+row_count_end].to_csv(f'./final_data/{category}/image_df_{iterations+1}.csv', sep = ';')

def assign_labels_to_features(feature_df):
    print('\n')
    pic_ids = feature_df.columns
    print('Extracting Column ids')
    feature_df.columns = [''] * len(feature_df.columns)
    print('labeling features...')
    column_labeled_features = create_col_labels_for_features(feature_df)
    print('Adding ids to features... ')
    id_labeled_features = add_ids_to_feature_df(column_labeled_features, pic_ids)
    print('Adding Categories to features')
    id_labeled_features['category_label'] = id_labeled_features.apply(labeler, axis=1)
    return id_labeled_features

if __name__ == '__main__':
    paths = get_img_features('../../capstone data/imgFeatures', 'animal')
    feature_df = load_all_image_features(paths)
    image_df = assign_labels_to_features(feature_df)
    print('saving csv files...')
    divide_save_csv(len(image_df), image_df, 'animal')
    # image_df.to_csv('./final_data/image_df.csv', sep = ';', index_label = False)
