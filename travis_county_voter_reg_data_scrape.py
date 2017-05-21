import csv
import pytz
import boto3
import boto.dynamodb2
from urllib.request import urlopen

from datetime import datetime
from bs4 import BeautifulSoup
from boto.dynamodb2.table import Table
from time import sleep


write_to_dynamo = True

url = 'http://travis.go2gov.net/showRptPrecincts.jsf'
voter_html = urlopen(url).read()
soup = BeautifulSoup(voter_html, 'html.parser')

voter_reg_table = soup.find(id='registrationByPrecinctSubView:registrationByPrecinctForm:_idJsp12')

voter_reg_table_rows = voter_reg_table.find_all('tr')

now = pytz.utc.localize(datetime.utcnow())
central = pytz.timezone('US/Central')
central_now = now.astimezone(central)
extraction_date = central_now.strftime('%Y%m%d')
extraction_time = central_now.strftime('%H%M%S')

connection = boto.dynamodb2.connect_to_region('us-east-2')
travis_reg = Table('TravisCountyVoterReg', connection=connection)
with open('travis_county_voter_{0}_{1}.csv'.format(extraction_date, extraction_time), 'w', newline='') as csvfile:
    voter_csv_writer = csv.writer(csvfile)
    headers = [c.contents[0] for c in voter_reg_table_rows[0].children] + ['Date (YMD)', 'Time (HMS)']
    voter_csv_writer.writerow(headers)
    for row in voter_reg_table_rows[1:]:
        # can't use .children here because of '\n' present between each 'td'
        row_contents = [c.contents[0].strip() for c in row.find_all('td')] + [extraction_date, extraction_time]
        voter_csv_writer.writerow(row_contents)

        # write to dynamo
        if write_to_dynamo:
            travis_reg.put_item(data={
                'precinct': row_contents[0] if row_contents[0] else 'unknown',
                'active': row_contents[1],
                'suspense': row_contents[2],
                'total': row_contents[3],
                'date': row_contents[4],
                'time': row_contents[5]
            })
            sleep(.2)

# store csv in s3
s3_client = boto3.session.Session(profile_name='s3').client('s3', 'us-east-2')
s3_client.upload_file('travis_county_voter_{0}_{1}.csv'.format(extraction_date, extraction_time),
                      'travis-county-voter-reg-collected',
                      'csvs/travis_county_voter_{0}_{1}.csv'.format(extraction_date, extraction_time))


all_records = travis_reg.scan()
with open('travis_county_voter_reg_collected.csv', 'w', newline='') as csvfile:
    voter_collected_csv_writer = csv.writer(csvfile)
    voter_collected_csv_writer.writerow(headers)
    for record in all_records:
        row_contents = [
            record['precinct'],
            record['active'],
            record['suspense'],
            record['total'],
            record['date'],
            record['time']
        ]
        voter_collected_csv_writer.writerow(row_contents)

# store csv in s3
s3_client.upload_file('travis_county_voter_reg_collected.csv',
                      'travis-county-voter-reg-collected',
                      'aggregated/travis_county_voter_reg_collected.csv',
                      ExtraArgs={'ACL': 'public-read'})
