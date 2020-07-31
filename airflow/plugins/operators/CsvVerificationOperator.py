from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from glob import glob as globlin ## The 7bb globlin
import pandas as pd
import logging

class CsvVerificationOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 path,
                 category,
                 *args, **kwargs):

        super(CsvVerificationOperator, self).__init__(*args, **kwargs)
        self.path = path
        self.category = category

    def get_csv_file_list(self, path):
        csv_list = globlin(path)
        if len(csv_list) >= 1:
            logging.info(f'{len(csv_list)} CSV files for {self.category}s found')
            return True, len(csv_list), csv_list
        else:
            raise ValueError(f"Data Quality Check failed. No CSV files found for category {self.category}.")

    def check_each_csv_count(self, path_list):
        for path in path_list:
            _, file_length = self.check_single_csv(path)
            logging.info(f'file -- {path} -- found with count {file_length}')

    def check_single_csv(self, path):
        single_df = pd.read_csv(path, sep = ';')
        if len(single_df) < 1:
            raise ValueError(f"Data Quality Check failed. No count found for file -- {path}.")
        else:
            return True, len(single_df)

    def execute(self, context):
        _, total_csv_length, csv_list = self.get_csv_file_list(f'{self.path}/{self.category}/*.*')
        self.check_each_csv_count(csv_list)
