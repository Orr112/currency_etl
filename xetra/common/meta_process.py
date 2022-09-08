import pandas as pd
from datetime import datetime, timedelta
from collections import Counter
from s3 import S3BucketConnector
from custom_exceptions import WrongMetaFileException
from constants import MetaProcessFormat
"""
Meta file processing 
"""

class MetaProcesFormat():

    def __init__(self):
        pass

    @staticmethod
    def update_meta_file(extract_date_list: list, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """
        Updating the meta file with the dates the Xetra files were updated and todays date as the processed date

        :param: extract_date_list - a list of dates thart are extracted from the source
        :param: meta_key - key of the meta file on the S3 bucket
        :param: s3_bucket_meta - S3BucketConnector for the bucket with the meta file
        """
        # Creating an empty DataFrame using the meta file column names
        df_new = pd.DataFrame(columns=[
            MetaProcessFormat.META_SOURCE_DATA_COL.value,
            MetaProcessFormat.META_PROCESS_COL.value])
        # Filling the date column with extract_date_list
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list
        # Filling the processed column
        df_new[MetaProcessFormat.META_PROCESS_COL.value] = \
            datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        try:
            # If meta file exists - union data frame of old and new meta data is created
            df_old = s3_bucket_meta.read_csv_to_df(meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFileException
            df_all = pd.concat([df_old, df_new])
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            # No meta file exists - only the new data is used
            df_all = df_new
        # Writing to S3
        s3_bucket_meta.write_df_to_s3(df_all, meta_key, MetaProcesFormat.META_FILE_FORMAT.value)
        return True

    @staticmethod
    def return_date_list():
        pass 