"""Xetra ETL"""
import logging
import pandas as pd
from typing import NamedTuple
from xetra.common.meta_process import MetaProcess
from xetra.common.s3 import S3BucketConnector


class XetraSourceConfig(NamedTuple):
    """
    Source configuration data

    
    src_first_extract_date: date of data to be extracted
    src_columns: list: source column names
    src_col_date: column name for date from source data
    src_col_isin: column name for isin from source data
    src_col_time: column name for time from source data
    src_col_start_price: column name for starting price in source data
    src_col_min_price: column name for minimum price in source data
    src_col_max_price: column name for maximum price in source data
    src_col_traded_vol: column name for traded volumen in source data
    """

    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str


class XetraTargetConfig:
    """
    Target configuration data

    trg_col_isin: column name for isin in target file
    trg_col_date: column name for date in target file
    trg_col_op_price: column name for opening price in target file
    trg_col_clos_price: column name for closing price in target file
    trg_col_min_price: column name for minimum price in target file
    trg_col_max_price: column name for maximum price in target file
    trg_col_dail_trad_vol: column name for daily traded volumne in target file
    trg_col_ch_prev_clos: column name for change in previous closing price in target ile
    trg_key: target file key
    trg_key_date_format: date format for target file key
    trg_format: target file format
    """
    trg_col_isin: str
    trg_col_date: str
    trg_col_op_price: str
    trg_col_clos_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_dail_trad_vol: str
    trg_col_ch_prev_clos: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str

class XetraETL():
    """
    Takes in, transform, and writes data to target 
    """

    def __init__(self, s3_bucket_src: S3BucketConnector, s3_bucket_trg: S3BucketConnector, 
                meta_key: str, src_args: XetraSourceConfig, trg_args: XetraTargetConfig):
        """
        Constructor for XetraTransformer

        :param s3_bucket_src: connection to source S3 bucket
        :param s3_bucket_trg: connection to targt S3 bucket
        :param meta_key: meta file key
        :param src_args: Named Touple class listed above for soruce configuration
        :param trg_args: Named Touple class listed above for target configuration
        """
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args
        self.extract_date, self.extract_date_list = MetaProcess.return_date_list(
            self.src_args.src_first_extract_date, self.meta_key, self.s3_bucket_trg)
        
        self.meta_update_list = None


    def extract(self):
        """
        Read the source data and concatenates the data into one DataFrame.

        :returns:
            data_frame: Padas DataFrame with the extracted data
        """
        self._logger.info('Extracting Xetra source files started....')
        files = [key for date in self.extract_date_list\
                    for key in self.s3_bucket_src.list_files_in_prefix(date)]
        if not files:
            data_frame = pd.DataFrame()
        else:
            data_frame = pd.concat([self.s3_bucket_src.read_csv_to_df(file)\
                for file in files], ignore_index=True)
        self._logger.info('Extracting Xetra source files finished.')
        return data_frame         


    def transform_report1():
        pass

    
    def load():
        pass


    def etl_report1():
        pass 