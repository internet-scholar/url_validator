# This script was used when I decided to create a table called tweet_user_url to unnest the URLs in each Tweet.
# To make backward compatibility possible I had to create the data files for previous dates (< 2019-11-28).

from internet_scholar import AthenaDatabase, compress
import boto3
from datetime import date, timedelta
import uuid

current_date = date(2019, 8, 15)
athena_db = AthenaDatabase(database='internet_scholar', s3_output='internet-scholar-admin')
query = """
select
  twitter_stream.id_str as tweet_id,
  twitter_stream.user.id_str as user_id,
  url.expanded_url as url
from
  internet_scholar.twitter_stream_raw as twitter_stream,
  unnest(entities.urls) as t(url)
where
  creation_date = '{creation_date}' and
  url.display_url not like 'twitter.com/%'
order by
  tweet_id,
  user_id,
  url;
"""

while current_date <= date(2019, 11, 27):
    print(str(current_date))
    tweet_user_url = athena_db.query_athena_and_download(query_string=query.format(creation_date=str(current_date)),
                                                         filename=str(current_date) + '.csv')
    compressed_file = compress(filename=tweet_user_url)

    s3 = boto3.resource('s3')
    s3_filename = "tweet_user_url/creation_date={creation_date}/{code}.csv.bz2".format(creation_date=str(current_date),
                                                                                       code=uuid.uuid4().hex)
    s3.Bucket('internet-scholar').upload_file(str(compressed_file), s3_filename)

    current_date = current_date + timedelta(days=1)