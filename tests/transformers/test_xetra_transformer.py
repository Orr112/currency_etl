import os
import unittest
from unittest.mock import patch

import boto3
import pandas as pd
from moto import mock_s3

from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.transformers.xetra_transformer import XetraETL, XetraSourceConfig, XetraTargetConfig

class TestXetraETLMethods(unittest.TestCase):
    """
    Testing the XetraETL class
    """

    def setUp(self):
        """
        Environment setup
        """
        # mocking S3 Connection setup
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.us-west-2.amazonaws.com'
        self.s3_bucket_name_src = 'src-bucket'
        self.s3_bucket_name_trg = 'trg-bucket'
        self.meta_key = 'meta_key'
        # Creating s3 access key as environment variable
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        # Creating a bucket on the mock S3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name_src, 
                                CreateBucketConfiguration={
                                    'LocationConstraint': 'us-west-2'
                                })
        self.s3.create_bucket(Bucket=self.s3_bucket_name_trg, 
                                CreateBucketConfiguration={
                                    'LocationConstraint': 'us-west-2'
                                })
        self.src_bucket = self.s3.Bucket(self.s3_bucket_name_src)
        self.trg_bucket = self.s3.Bucket(self.s3_bucket_name_trg)
        # Creating a testing instance
        self.s3_bucket_src = S3BucketConnector(self.s3_access_key, 
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name_src)
        self.s3_bucket_trg = S3BucketConnector(self.s3_access_key, 
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name_trg)
        # Creating source and target configuration
        conf_dict_src = {
            'src_first_extract_date':'2021-04-01',
            'src_columns':['ISIN', 'Mnemonic','Date','Time',
            'StartPrice','EndPrice','MinPrice','MaxPrice','TradedVolume'],
            'src_col_date': 'Date',
            'src_col_isin': 'ISIN',
            'src_col_time': 'Time',
            'src_col_start_price': 'StartPrice',
            'src_col_min_price': 'MinPrice',
            'src_col_max_price': 'MaxPrice',
            'src_col_traded_vol': 'TradedVolume'
        }
        conf_dict_trg = {
            'trg_col_isin': 'isin',
            'trg_col_date': 'date',
            'trg_col_op_price': 'opening_price_eur',
            'trg_col_clos_price': 'closing_price_eur',
            'trg_col_min_price': 'minimum_price_eur',
            'trg_col_max_price': 'maximum_price_eur',
            'trg_col_dail_trad_vol': 'daily_traded_volume',
            'trg_col_ch_prev_clos': 'change_prev_closing_%',
            'trg_key': 'report1/xetra_daily_report1_',
            'trg_key_date_format': '%Y%m%d_%H%M%S',
            'trg_format': 'parquet'
        }
        self.source_config = XetraSourceConfig(**conf_dict_src)
        self.target_config = XetraTargetConfig(**conf_dict_trg)
        # Creating souce files on mocked s3
        columns_src = ['ISIN','Mnemonic','Date','Time','StartPrice',
        'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume']
        data = [['AT0000A0E9W5', 'SANT', '2022-09-14', '12:00', 20.19, 18.45, 18.20, 20.33, 877],
                ['AT0000A0E9W5', 'SANT', '2022-09-15', '15:00', 18.27, 21.19, 18.27, 21.34, 987],
                ['AT0000A0E9W5', 'SANT', '2022-09-16', '13:00', 20.21, 18.27, 18.21, 20.42, 633],
                ['AT0000A0E9W5', 'SANT', '2022-09-16', '14:00', 18.27, 21.19, 18.27, 21.34, 455],
                ['AT0000A0E9W5', 'SANT', '2022-09-17', '07:00', 20.58, 19.27, 18.89, 20.58, 9066],
                ['AT0000A0E9W5', 'SANT', '2022-09-17', '08:00', 19.27, 21.14, 19.27, 21.14, 1220],
                ['AT0000A0E9W5', 'SANT', '2022-09-18', '07:00', 23.58, 23.58, 23.58, 23.58, 1035],
                ['AT0000A0E9W5', 'SANT', '2022-09-18', '08:00', 23.58, 24.22, 23.31, 24.34, 1028],
                ['AT0000A0E9W5', 'SANT', '2022-09-18', '08:00', 24.22, 22.21, 22.21, 25.01, 1523]
        ]
        self.df_src = pd.DataFrame(data, columns=columns_src)
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[0:0],
        '2022-09-14/2022-09-15_BINS_XETR12.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[1:1],
        '2022-09-15/2022-09-16_BINS_XETR15.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[2:2],
        '2022-09-16/2022-09-16_BINS_XETR13.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[3:3],
        '2022-09-16/2022-09-16_BINS_XETR14.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[4:4],
        '2022-09-17/2022-09-17_BINS_XETR07.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[5:5],
        '2022-09-17/2022-09-17_BINS_XETR08.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[6:6],
        '2022-09-18/2022-09-18_BINS_XETR07.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[7:7],
        '2022-09-18/2022-09-18_BINS_XETR08.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[8:8],
        '2022-09-18/2022-09-18_BINS_XETR08.csv','csv')
        columns_report = ['ISIN', 'Date', 'opening_price_eur','closing_price_eur',
        'minimum_price_eur','maximum_price_eur', 'daily_traded_volume', 'change_prev_closing_%']
        data_report = [['AT0000A0E9W5', '2022-09-16', 20.21, 18.27, 18.21, 21.34, 1088, 10.62],
                        ['AT0000A0E9W5', '2022-09-17', 20.58, 19.27, 18.89, 21.14, 10286, 1.83],
                        ['AT0000A0E9W5', '2022-09-18', 23.58, 24.22, 22.21, 25.01, 3586, 14.58]]
        self.df_report = pd.DataFrame(data_report, columns=columns_report)

    def tearDown(self):
        """
        Completed after unit tests have been completed
        """
        # mocking termination of s3 connection
        self.mock_s3.stop()
    
    def test_extract_no_files(self):
        """
        Test the extract method when
        there are no files to be extracted
        """
        extract_date = '2200-01-02'
        extract_date_list = []
        with patch.object(MetaProcess, "return_date_list",
        return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.s3_bucket_src, self.s3_bucket_trg,
                            self.meta_key, self.source_config, self.target_config)
            df_return = xetra_etl.extract()
        self.assertTrue(df_return.empty)

    def test_extract_files(self):
        """
        Tests the extract method when
        there are files to be extracted
        """
        # Expected results
        df_exp = self.df_src.loc[1:8].reset_index(drop=True)
        # Test init
        extract_date = '2022-09-16'
        extract_date_list = ['2022-09-15', '2022-09-16', '2022-09-17', '2022-09-18', '2022-09-19']
        # Method execution
        with patch.object(MetaProcess, "return_date_list",
        return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.s3_bucket_src, self.s3_bucket_trg,
                            self.meta_key, self.source_config, self.target_config)
            df_result = xetra_etl.extract()
        # Test after method execution
        self.assertTrue(df_exp.equals(df_result))

if __name__ == '__main__':
    unittest.main()