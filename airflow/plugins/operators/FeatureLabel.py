from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import numpy as np
import pandas as pd
from glob import glob as globlin ## The 7bb globlin

class FeatureLabelOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 main_path,
                 output_path,
                 *args, **kwargs):

        super(FeatureLabelOperator, self).__init__(*args, **kwargs)
        self.main_path = main_path
        self.output_path = output_path

    def get_img_features(self, main_path):
        feature_paths = globlin(main_path + '/*/*.*')
        return feature_paths

    def load_all_image_features(self, directories):
        feature_list = []
        for index, directory in enumerate(self, directories):
            column_name = str(directory.split('/')[-1].replace('.npz',''))
            print(f'Loading features for {column_name} -- {index}/{len(directories)}', end='\r')
            feature_list.append(pd.DataFrame(np.loadtxt(directory), columns = [column_name]))
        return pd.concat(feature_list, axis = 1)

    def create_col_labels_for_features(self, feature_df):
        feature_df_columns = []
        number_of_features = len(feature_df)
        for idx in range(0, number_of_features):
            feature_df_columns.append('img_feature_' + str(idx+1))
        final_feature_df = feature_df.T
        final_feature_df.columns = feature_df_columns
        return final_feature_df

    def add_ids_to_feature_df(self, feature_df, pic_ids):
        pic_column = pd.DataFrame(pic_ids, columns = ['picID'])
        pic_column.reset_index(drop=True, inplace=True)
        feature_df.reset_index(drop=True, inplace=True)
        return pd.concat([pic_column, feature_df], axis = 1)

    def labeler(self, row):
        if 'plant' in row['picID']:
            return 0
        elif 'animal' in row['picID']:
            return 1
        elif 'human' in row['picID']:
            return 2

    def assign_labels_to_features(self, feature_df):
        print('\n')
        pic_ids = feature_df.columns
        feature_df.columns = [''] * len(feature_df.columns)
        column_labeled_features = self.create_col_labels_for_features(feature_df)
        id_labeled_features = self.add_ids_to_feature_df(column_labeled_features, pic_ids)
        id_labeled_features['category_label'] = id_labeled_features.apply(self.labeler, axis=1)
        return id_labeled_features

    def create_csv(self, context):
        paths = self.get_img_features(self.main_path)
        feature_df = self.load_all_image_features(paths)
        image_df = self.assign_labels_to_features(feature_df)
        image_df.to_csv(self.output_path + '/image_df.csv', sep = ';', index_label = False)