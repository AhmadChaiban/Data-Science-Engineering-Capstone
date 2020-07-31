from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from glob import glob as globlin ## The 7bb globlin
import logging
import threading
import boto3
import os
import sys
from boto3.s3.transfer import TransferConfig
import configparser

class s3UploadOperator(BaseOperator):
    ui_color = '#89DA59'
    @apply_defaults
    def __init__(self,
                 path,
                 category,
                 bucket_name,
                 *args, **kwargs):

        super(s3UploadOperator, self).__init__(*args, **kwargs)
        self.path = path
        self.category = category
        self.bucket_name = bucket_name

    def get_key_secret(self):
        """
            Getting key and secret from the config fields

            :return: AWS Access key id,
                     AWS secrety access key
        """
        config = configparser.ConfigParser()
        config.read('../../../AWS_Tools/dl.cfg')
        KEY = config['AWS']['AWS_ACCESS_KEY_ID']
        SECRET = config['AWS']['AWS_SECRET_ACCESS_KEY']
        return KEY, SECRET

    def get_bucket(self, bucket_name, KEY, SECRET, region = None):

        s3_resource = boto3.resource('s3',
                                     region_name=region,
                                     aws_access_key_id=KEY,
                                     aws_secret_access_key=SECRET)

        return s3_resource

    # BUCKET_NAME = "capstone-project-2187"
    def multi_part_upload_with_s3(self, file_path, key_path, BUCKET_NAME, KEY, SECRET):
        s3_resource = self.get_bucket(BUCKET_NAME, KEY, SECRET, region = None)
        # Multipart upload
        config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                                multipart_chunksize=1024 * 25, use_threads=True)

        s3_resource.meta.client.upload_file(file_path,
                                            BUCKET_NAME,
                                            key_path,
                                            ExtraArgs={'ACL': 'public-read', 'ContentType': 'CSV'},
                                            Config=config)

    def execute(self, context):
        KEY, SECRET = self.get_key_secret()
        file_list = globlin(self.path + f'/{self.category}/*.*', recursive=True)
        for file in file_list:
            try:
                key_path = 'raw_data' + '/' + file.split('/')[-2] + '/' + file.split('/')[-1]
                multi_part_upload_with_s3(file, key_path, self.bucket_name)
                logging.info(f'File upload -- {file} -- Completed successfully.')
            except:
                raise ValueError(f'Upload for file -- {file} -- failed.')
