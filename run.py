"""
Entry point for ETL application
"""
import logging
import logging.config

import yaml

def main():
    # Parsing YAML file
    config_path = 'C:/Users/omarr/Documents/udemy_etl_prj/currency_etl/configs/xetra_report1_config.yml'
    config = yaml.safe_load(open(config_path))
    print(config)
    # Configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is a test.")


if __name__ == '__main__':
    main()