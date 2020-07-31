from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from glob import glob as globlin ## The 7bb globlin
import logging

class FeatureVerificationOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 path,
                 category,
                 *args, **kwargs):

        super(FeatureVerificationOperator, self).__init__(*args, **kwargs)
        self.path = path
        self.category = category

    def get_feature_file_list(self, path):
        feature_list = globlin(path + f'/{self.category}/*.*')
        if len(feature_list) >= 1:
            logging.info(f'Data Quality Check passed. Found {len(feature_list)} features for {self.category}s')
        else:
            raise ValueError(f"Data Quality Check failed. No features found for category {self.category}.")

    def execute(self, context):
        self.get_feature_file_list(self.path)