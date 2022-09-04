import boto3
import csv
import logging
import time
from internet_scholar import AthenaLogger, AthenaDatabase, compress, read_dict_from_s3_url, URLExpander
import argparse
from pathlib import Path
import shutil
from datetime import date, timedelta
import uuid


class URLValidator:
    __TWEET_USER_URL = """
    select
      twitter_stream.id_str as tweet_id,
      twitter_stream.user.id_str as user_id,
      url.expanded_url as url
    from
      internet_scholar.twitter_stream as twitter_stream,
      unnest(entities.urls) as t(url)
    where
      twitter_stream.creation_date = '{creation_date}' and
      url.display_url not like 'twitter.com/%'
    order by
      tweet_id,
      user_id,
      url;    
    """

    __UNVALIDATED_URLS = """
    select
        distinct url
    from
        tweet_user_url
    where
        creation_date = '{creation_date}'
    """

    __COUNT_UNVALIDATED_URLS = """
    select
        count(distinct url) as link_count
    from
        tweet_user_url
    where
        creation_date = '{creation_date}'
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
       content_length bigint,
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

    __CREATE_TABLE_TWEET_USER_URL = """
    CREATE EXTERNAL TABLE if not exists tweet_user_url (
       tweet_id string,
       user_id string,
       url string
    )
    PARTITIONED BY (creation_date String)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
       'separatorChar' = ',',
       'quoteChar' = '"',
       'skip.header.line.count' = '1'
       )
    STORED AS TEXTFILE
    LOCATION 's3://{s3_data}/tweet_user_url/';
    """

    LOGGING_INTERVAL = 1000

    def __init__(self, s3_admin, s3_data, athena_data):
        self.athena_data = athena_data
        self.s3_data = s3_data
        self.s3_admin = s3_admin

    def expand_urls(self, creation_date=None):
        logging.info("begin: expand URLs")
        athena = AthenaDatabase(database=self.athena_data, s3_output=self.s3_admin)

        yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        if creation_date is None:
            creation_date = yesterday
        logging.info("Expand URLs that were tweeted on {creation_date}".format(creation_date=creation_date))

        query_tweet_user_url = self.__TWEET_USER_URL.format(creation_date=creation_date)
        query = self.__UNVALIDATED_URLS.format(creation_date=creation_date)
        query_count = self.__COUNT_UNVALIDATED_URLS.format(creation_date=creation_date)
        if athena.table_exists("validated_url"):
            logging.info("Table validated_url exists")
            query = query + " and url not in (select validated_url.url from validated_url)"
            query_count = query_count + " and url not in (select validated_url.url from validated_url)"

        logging.info('Update table tweet_user_url')
        tweet_user_url = athena.query_athena_and_download(
            query_string=query_tweet_user_url.format(creation_date=creation_date),
            filename=creation_date + '.csv')
        compressed_file = compress(filename=tweet_user_url)
        s3 = boto3.resource('s3')
        s3_filename = "tweet_user_url/creation_date={creation_date}/{code}.csv.bz2".format(creation_date=creation_date,
                                                                                           code=uuid.uuid4().hex)
        logging.info('Upload data file that will comprise tweet_user_url')
        s3.Bucket(self.s3_data).upload_file(str(compressed_file), s3_filename)

        logging.info('Update table tweet_user_url on Athena')
        logging.info("Create Athena table tweet_user_url if does not exist already")
        athena.query_athena_and_wait(query_string=self.__CREATE_TABLE_TWEET_USER_URL.format(s3_data=self.s3_data))
        athena.query_athena_and_wait(query_string="MSCK REPAIR TABLE tweet_user_url")

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
                    for expanded_url in url_expander.expand_url(url['url']):
                        writer.writerow(expanded_url)
                logging.info("All links processed")

        logging.info("Compress file %s", validated_urls)
        compressed_file = compress(filename=validated_urls, delete_original=True)

        if creation_date == yesterday:
            filename_s3 = 'validated_url_raw/{}-{}.csv.bz2'.format(
                time.strftime('%Y-%m-%d-%H-%M-%S', time.gmtime()), link_count)
        else:
            filename_s3 = 'validated_url_raw/{}-{}.csv.bz2'.format(
                creation_date + '-23-59-59', link_count)
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
    parser.add_argument('--creation_date', help='If specified, script will validate URLs for given date; '
                                                'otherwise, it will assume yesterday')
    args = parser.parse_args()

    config = read_dict_from_s3_url(url=args.config)
    logger = AthenaLogger(app_name="url_validator",
                          s3_bucket=config['aws']['s3-admin'],
                          athena_db=config['aws']['athena-admin'])
    try:
        url_validator = URLValidator(s3_admin=config['aws']['s3-admin'],
                                     s3_data=config['aws']['s3-data'],
                                     athena_data=config['aws']['athena-data'])
        url_validator.expand_urls(creation_date=args.creation_date)

        total, used, free = shutil.disk_usage("/")
        logging.info("Disk Usage: total: %.1f Gb - used: %.1f Gb - free: %.1f Gb",
                     total / (2**30), used / (2**30), free / (2**30))
    finally:
        logger.save_to_s3()
        # logger.recreate_athena_table()


if __name__ == '__main__':
    main()
