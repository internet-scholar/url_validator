import boto3
import csv
import logging
import time
from internet_scholar import AthenaLogger, AthenaDatabase, compress, read_dict_from_s3_url, URLExpander
import argparse
from pathlib import Path
import shutil


class URLValidator:
    __UNVALIDATED_URLS = """
    select
        distinct url.expanded_url
    from
        twitter_stream,
        unnest(entities.urls) as t(url)
    where
        url.display_url not like 'twitter.com/%'
    """

    __COUNT_UNVALIDATED_URLS = """
    select
        count(distinct url.expanded_url) as link_count
    from
        twitter_stream,
        unnest(entities.urls) as t(url)
    where
        url.display_url not like 'twitter.com/%'
    """

    __CREATE_TABLE_VALIDATED_URL = """
    CREATE TABLE validated_url
    WITH (external_location = 's3://{s3_data}/validated_url/',
          format = 'PARQUET',
          bucketed_by = ARRAY['url'],
          bucket_count=1) AS
    select url, validated_url, status_code, content_type, content_length, created_at
    from (select t.*,
                 row_number() over (partition by url order by created_at) as seqnum
          from validated_url_raw t
         ) t
    where seqnum = 1
    """

    __CREATE_TABLE_VALIDATED_URL_RAW = """
    CREATE EXTERNAL TABLE if not exists validated_url_raw (
       url string,
       validated_url string,
       status_code int,
       content_type string,
       content_length int,
       created_at timestamp
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
       'separatorChar' = ',',
       'quoteChar' = '"'
       )
    STORED AS TEXTFILE
    LOCATION 's3://{s3_data}/validated_url_raw/';
    """

    LOGGING_INTERVAL = 1000

    def __init__(self, s3_admin, s3_data, athena_data):
        self.athena_data = athena_data
        self.s3_data = s3_data
        self.s3_admin = s3_admin

    def expand_urls(self):
        logging.info("begin: expand URLs")
        athena = AthenaDatabase(database=self.athena_data, s3_output=self.s3_admin)

        query = self.__UNVALIDATED_URLS
        query_count = self.__COUNT_UNVALIDATED_URLS
        if athena.table_exists("validated_url"):
            athena.query_athena_and_wait("MSCK REPAIR TABLE validated_url")
            logging.info("Table validated_url does not exist")
            query = query + " and url.expanded_url not in (select url from validated_url)"
            query_count = query_count + " and url.expanded_url not in (select url from validated_url)"
        link_count = int(athena.query_athena_and_get_result(query_string=query_count)['link_count'])
        logging.info("There are %d links to be processed: download them", link_count)
        unvalidated_urls = athena.query_athena_and_download(query_string=query, filename="unvalidated_urls.csv")

        with open(unvalidated_urls, newline='') as csv_reader:
            validated_urls = Path(Path(__file__).parent, 'tmp', 'validated_urls.csv')
            Path(validated_urls).parent.mkdir(parents=True, exist_ok=True)
            logging.info("Create file %s for validated URLs", validated_urls)
            with open(str(validated_urls), 'w') as csv_writer:
                reader = csv.DictReader(csv_reader)
                writer = csv.DictWriter(
                    csv_writer,
                    fieldnames=['url', 'validated_url', 'status_code', 'content_type', 'content_length', 'created_at'],
                    dialect='unix'
                )
                url_expander = URLExpander()
                num_links = 0
                for url in reader:
                    if num_links % self.LOGGING_INTERVAL == 0:
                        logging.info("%d out of %d links processed", num_links, link_count)
                    num_links = num_links + 1
                    for expanded_url in url_expander.expand_url(url['expanded_url']):
                        writer.writerow(expanded_url)
                logging.info("All links processed")

        logging.info("Compress file %s", validated_urls)
        compressed_file = compress(filename=validated_urls, delete_original=True)

        s3 = boto3.resource('s3')
        filename_s3 = 'validated_url_raw/{}-{}.csv.bz2'.format(
            time.strftime('%Y-%m-%d-%H-%M-%S', time.gmtime()), link_count)
        logging.info("Upload file %s to bucket %s at %s", compressed_file, self.s3_data, filename_s3)
        s3.Bucket(self.s3_data).upload_file(str(compressed_file), filename_s3)

        logging.info("Delete previous validated_url data: will be generated again")
        s3.Bucket(self.s3_data).objects.filter(Prefix="validated_url/").delete()

        logging.info("Create Athena table validated_url_raw if does not exist already")
        athena.query_athena_and_wait(query_string=self.__CREATE_TABLE_VALIDATED_URL_RAW.format(s3_data=self.s3_data))
        logging.info("Drop Athena table validated_url")
        athena.query_athena_and_wait(query_string="drop table if exists validated_url")
        logging.info("Creates Athena table validated_url through CTAS")
        athena.query_athena_and_wait(query_string=self.__CREATE_TABLE_VALIDATED_URL.format(s3_data=self.s3_data))
        logging.info("END: expand URLs")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='S3 Bucket with configuration', required=True)
    args = parser.parse_args()

    config = read_dict_from_s3_url(url=args.config)
    logger = AthenaLogger(app_name="url_validator",
                          s3_bucket=config['aws']['s3-admin'],
                          athena_db=config['aws']['athena-admin'])
    try:
        url_validator = URLValidator(s3_admin=config['aws']['s3-admin'],
                                     s3_data=config['aws']['s3-data'],
                                     athena_data=config['aws']['athena-data'])
        url_validator.expand_urls()

        total, used, free = shutil.disk_usage("/")
        logging.info("Disk Usage: total: %.1f Gb - used: %.1f Gb - free: %.1f Gb",
                     total / (2**30), used / (2**30), free / (2**30))
    finally:
        logger.save_to_s3()
        logger.recreate_athena_table()


if __name__ == '__main__':
    main()
