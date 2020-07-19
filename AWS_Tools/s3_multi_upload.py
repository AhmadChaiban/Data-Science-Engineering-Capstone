import threading
import boto3
import os
import sys
from boto3.s3.transfer import TransferConfig
from s3_create import get_key_secret, get_bucket


KEY, SECRET = get_key_secret()

s3_resource = get_bucket('capstone-project-2187', KEY, SECRET, region = None)

BUCKET_NAME = "capstone-project-2187"
def multi_part_upload_with_s3(file_path, key_path):
    # Multipart upload
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)

    s3_resource.meta.client.upload_file(file_path, BUCKET_NAME, key_path,
                               ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/pdf'},
                               Config=config,
                               Callback=ProgressPercentage(file_path)
                               )

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

# if __name__ == '__main__':
#     multi_part_upload_with_s3()