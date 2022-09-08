import os
import os
import boto3
import unittest
import pandas as pd
from tkinter import N
from moto import mock_s3
from io import StringIO, BytesIO
from datetime import datetime, timedelta
from xetra.common.constants import MetaProcessFormat
from xetra.common.meta_process import MetaProcesFormat
from xetra.common.custom_exceptions import WrongFormatException, WrongMetaFileException
from xetra.common.s3 import S3BucketConnector

class TestMetaProcessMethods(unittest.TestCase):
    """
    Testing the MetaProcess class.
    """

    def setUp(self):
        """
        Setting up the environment
        """
        # mocking s3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.us-west-2.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        # Createing s3 access key as environment variable
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        # creating a bucket on the mock s3
        self.s3 = boto3.resources(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                                CreateBucketConfiguration={
                                    'LocationConstraint': 'us-west-2'})
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # Creating a S3BucketConnector instance
        self.s3_bucket_meta = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)
        self.dates = [(datetime.today().date() - timedelta(days=day))\
            .strftime(MetaProcessFormat.META_DATE_FOMAT.value) for day in range(8)]
    
    def tearDown(self):
        # mocking s3 connection stop
        self.mock_s3.stop()

    def test_update_meta_file_no_meta_file(self):
        """
        Tests the update_meta_file method
        when the meta file does not exists
        """
        # Expected results
        date_list_exp = ['2022-09-05','2022-09-06']
        proc_date_list_exp = [datetime.today().date()] * 2
        # Test init
        meta_key = 'meta.csv'
        # Method execution
        MetaProcessFormat.update_meta_file(date_list_exp, meta_key, self.s3_bucket_meta)
        # Read meta file
        data = self.s3_bucket.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_meta_results = pd.read_csv(out_buffer)
        date_list_result = list(df_meta_results[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        proc_date_list_result = list(
            pd.to_datetime(df_meta_results[MetaProcessFormat.META_PROCESS_COL.value])\
            .dt.date
        )
        # Test after method execution
        self.assertEqual(date_list_exp, date_list_result)
        self.assertEqual(proc_date_list_exp, proc_date_list_result)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                    
                ]

            }
        )

    def test_update_meta_file_empty_date_list(self):
        """
        Test the update_meta_file method when extract_date list is empty
        """
        # Expected results
        return_exp = True
        log_exp = 'The dataframe is empty! No file will be written'
        # Test init
        date_list = []
        meta_key = 'meta.csv'
        # Method execution
        with self.assertLogs() as logm:
            result = MetaProcessFormat.update_meta_file(date_list, meta_key, self.s3_bucket_meta)
            # Log test after method execution
            self.assertIn(log_exp, logm.output[1])
        self.assertEqual(return_exp, result)

    def test_update_meta_file_meta_file_ok(self):
        """
        Test the update_meta_file method when there is aleady a meta file
        """
        # Expected results
        date_list_old = ['2022-09-05','2022-09-06']
        date_list_new = ['2022-09-07','2022-09-08']
        date_list_exp = date_list_old + date_list_new
        proc_date_list_exp = [datetime.today().date()] * 4
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
            f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
            f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{date_list_old[0]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
            f'{date_list_old[1]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}'
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        # Method execution
        MetaProcessFormat.update_meta_file(date_list_new, meta_key, self.s3_bucket_meta)
        # Read meta file
        data = self.s3_bucket.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result = list(df_meta_result[
            MetaProcessFormat.META_SOURCE_DATE_COV.value])
        proc_date_list_result = list(pd.to_datetime(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value])\
                               .dt.date)
        # Test after method execution
        self.assertEqual(date_list_exp, date_list_result)
        self.assertEquql(proc_date_list_exp, proc_date_list_result)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_update_meta_file_meta_file_wrong(self):
        """
        Tests the update_meta_file method when the meta file is incorrect
        """
        # Expected results
        date_list_old = ['2022-09-02', '2022-09-03']
        date_list_new = ['2022-09-06', '2022-09-07']
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
            f'wrong_column,{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{date_list_old[0]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
            f'{date_list_old[1]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}'
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        # Method execution
        with self.assertRaises(WrongMetaFileException):
            MetaProcessFormat.update_meta_file(date_list_new, meta_key, self.s3_bucket_meta)
        # Cleanup afte test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )
