import logging
import boto3
import numpy as np
from botocore.exceptions import ClientError
## The 7bb globlin
from glob import glob as globlin
import configparser
#from s3_parallel_uploads import execute_parallel_uploads


def get_key_secret():

    """
        Getting key and secret from the config fields

        :return: AWS Access key id,
                 AWS secrety access key
    """
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    KEY = config['AWS']['AWS_ACCESS_KEY_ID']
    SECRET = config['AWS']['AWS_SECRET_ACCESS_KEY']
    return KEY, SECRET


def create_bucket(bucket_name, KEY, SECRET, region=None):
    """
        Create an S3 bucket in us-west-2

        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :param KEY: The AWS access key
        :param SECRET: AWS secret access key
        :return: s3_client upon creation, exit otherwise
    """
    ## Creating the bucket
    try:
        if region is None:
            s3_client = boto3.client('s3',
                                     aws_access_key_id=KEY,
                                     aws_secret_access_key=SECRET)
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3',
                                     region_name=region,
                                     aws_access_key_id=KEY,
                                     aws_secret_access_key=SECRET)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        print('Could not create')
        exit()

    print('************************************')
    print('Create S3 Client')
    print('************************************')
    return s3_client

def get_bucket(bucket_name, KEY, SECRET, region = None):

    s3_resource = boto3.resource('s3',
                             region_name=region,
                             aws_access_key_id=KEY,
                             aws_secret_access_key=SECRET)

    return s3_resource

if __name__ == '__main__':
    KEY, SECRET = get_key_secret()
    create_bucket('capstone-project-2187', KEY, SECRET, region=None)




