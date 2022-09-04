"""Connector and methods for acessing S3"""
import os
import logging
import pandas as pd
from io import StringIO, BytesIO
from xetra.common.constants import S3FileTypes
from xetra.common.custom_exceptions import WrongFormatException

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
        self._logger.info('Reading file %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(encoding)
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=sep)
        return df

    def write_df_to_s3(self, data_frame: pd.DataFrame,  key: str, file_format:str):
        """
        Method to write a Pandas DataFrame to S3

        :data_frame: Pandas DataFrame
        :key: target key 
        :file_format: format specification for the saved file
        """
        if data_frame.empty:
            self._logger.info('The dataframe is empty! No file will be written')
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        self._logger.info('The file format %s is not supported to be written to s3!', file_format)
        raise WrongFormatException

    def __put_object(self, out_bufer: StringIO or BytesIO, key: str):
        """
        Helper fuction for self.write_df_to_s3()

        :out_buffer: StringIO | BytesIO written to file
        :key: target key of the saved file
        """

        self._logger.info('Writing file to %s/%s/%s', self.endpoint_url, self._bucket.name, key)



