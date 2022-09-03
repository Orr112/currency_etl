"""Connector and methods for acessing S3"""
import os
import logging
import pandas as pd
from io import StringIO, BytesIO

import boto3


class S3BucketConnector():
    """
    Class to interact with S3 Buckets
    """
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str ):
        """
        S3BucketConnector Constructor

        :param access_key: access key for to access S3
        :param secret_key: secret key to access S3
        :param endpoint_url: endpoint url to S3
        :param  bucket: S3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                    aws_secret_access_key=os.environ[secret_key])
        self._s3 = self.session.resource(service_name='s3', endpoint_url =endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix: str):

        """
        listing of files with a matching prefix in the S3 bucket

        :param prefix: prefix on the S3 bucket

        return: 
            files: list of all file names matching the prefix
        """
        files =[obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(self, key: str,  encoding: str = 'utf-8', sep: str =','):
        """
        Csv file from S3 into a dataframe

        :param key: file key
        :encoding: encoding file data into csv readable format
        :sep: separator used in file

        returns:
            data_frame: Pandas DataFrame with data from file extracted from S3
        """
        self._logger.info('Readign file %s/%s%s', self.endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Objects(key=key).get().get('Body').read().decode(encoding)
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=sep)
        return df

    def write_df_to_s3(self, key: str, bucket, df):
        out_buffer = BytesIO()
        df.to_csv(out_buffer, index=False)
        bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True



