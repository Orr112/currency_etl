from dataclasses import dataclass 

@dataclass
class XetraSourceConfig:
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
    trg_col_isin: str
    trg_col_date: list
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

    def __init__(self, bucket_src, bucket_trg, meta_key, src_args, trg_args):
        self.s3_bucket_src = bucket_src
        self.s3_bucket_trg = bucket_trg
        self.meta_key = meta_key
        self.src_args = XetraSourceConfig
        self.trg_args = XetraTargetConfig
        self.extract_date = 'string'
        self.extract_date_list = []
        self.update_date_list = []


    def extract():
        pass


    def transform_report1():
        pass

    
    def load():
        pass


    def etl_report1():
        pass 