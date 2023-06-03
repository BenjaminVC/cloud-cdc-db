import json
import boto3
import psycopg2
import os
import xml.etree.ElementTree as ET
from urllib.parse import unquote_plus

print("imports done")

# S3 client set up
s3_client = boto3.client('s3', region_name='us-west-2')

try:
    print("s3 client set up done")

    buckets = s3_client.list_buckets()
    for bucket in buckets['Buckets']:
        print(f'Bucket Name: {bucket["Name"]}')

except Exception as e:
    print(f'An error occurred: {e}')

# RDS/PostgreSQL set up
rds_host = 'cdcdb.cvjwlz3ovg9l.us-west-2.rds.amazonaws.com'
rds_port = '5432'
rds_db_name = 'vaccineDB'
rds_user = 'postgres'
rds_password = 'cs455pass'

print("rds set up done")


# parses xml/json for lambda_handler DB query
def parse_file(content, file_type):
    print("parsing")

    if file_type == "xml":
        tree = ET.fromstring(content)
        site = tree.find('site')
        data = {
            "SiteID": int(site.get('id')),
            "Name": site.find('name').text,
            "ZipCode": site.find('zipCode').text,
            "Date": f"{tree.get('year')}-{tree.get('month')}-{tree.get('day')}",
            "FirstShot": sum(int(brand.find('firstShot').text) for brand in tree.findall('vaccines/brand')),
            "SecondShot": sum(int(brand.find('secondShot').text) for brand in tree.findall('vaccines/brand')),
        }
    elif file_type == "json":
        parsed_json = json.loads(content)
        data = {
            "SiteID": int(parsed_json['site']['id']),
            "Name": parsed_json['site']['name'],
            "ZipCode": parsed_json['site']['zipCode'],
            "Date": f"{parsed_json['date']['year']}-{parsed_json['date']['month']}-{parsed_json['date']['day']}",
            "FirstShot": sum(vaccine['firstShot'] for vaccine in parsed_json['vaccines']),
            "SecondShot": sum(vaccine['secondShot'] for vaccine in parsed_json['vaccines']),
        }
    else:
        # this shouldn't happen... but if it does
        raise ValueError("Invalid file type:", file_type)
    print("parsing complete")
    return data


def lambda_handler(event, context):
    # establishes new DB connection every time the lambda is invoked
    conn = psycopg2.connect(database=rds_db_name, user=rds_user, password=rds_password, host=rds_host, port=rds_port, )

    print("db connection established")
    # get the name of the bucket that triggered the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']

    print("bucket fetched")

    # get the file key and decode the special characters using unquote_plus import
    key = unquote_plus(event['Records'][0]['s3']['object']['key'])

    print("file key stored =", key, "from bucket =", bucket_name)

    # now get the file type from the tag
    tagging = s3_client.get_object_tagging(Bucket=bucket_name, Key=key)
    print("tagging =", tagging)
    print("file type from tag stored")

    # because we only ever associate one tag with our uploads, we can get the 0th tag from the 'TagSet' list
    file_type = tagging['TagSet'][0]['Value']
    print("file type =", file_type)

    # get the object and content from the obj var
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    print(key, "content stored.")
    file_content = obj['Body'].read().decode('utf-8')

    # parse the file and extract xml or json data
    data = parse_file(file_content, file_type)

    # SQL queries, inserts data into DB
    # ... note, this uses s% for placeholder for real value type. type is determined when the query executes
    # uses 'UPSERT' approach using the INSERT INTO and ON CONFLICT DO NOT UPDATE construct'
    # ... note, an UPSERT operation helps to avoid duplicate entries while ensuring that the data is the most recent
    # SQL queries, inserts data into DB
    # ... note, this uses s% for placeholder for real value type. type is determined when the query executes
    # uses 'UPSERT' approach using the INSERT INTO and ON CONFLICT DO NOT UPDATE construct'
    # ... note, an UPSERT operation helps to avoid duplicate entries while ensuring that the data is the most recent
    try:
        with conn.cursor() as cursor:
            site_id = data["SiteID"]
            name = data["Name"]
            zip_code = data["ZipCode"]
            first_shot = data["FirstShot"]
            second_shot = data["SecondShot"]
            vaccination_date = data["Date"]

            site_query = """INSERT INTO Sites (SiteID, Name, ZipCode) VALUES (%s, %s, %s) 
                            ON CONFLICT (SiteID) DO UPDATE SET Name = %s, ZipCode = %s;"""
            cursor.execute(site_query, (site_id, name, zip_code, name, zip_code))

            data_query = """INSERT INTO Data (SiteID, Date, FirstShot, SecondShot) VALUES (%s, %s, %s, %s) 
                            ON CONFLICT (SiteID, Date) DO UPDATE SET FirstShot = %s, SecondShot = %s;"""
            cursor.execute(data_query, (site_id, vaccination_date, first_shot, second_shot, first_shot, second_shot))
            # commit changes to the db
            conn.commit()
    except Exception as err:
        print(err)
        raise err
    finally:
        # close connection to RDS/PostgreSQL
        if conn is not None:
            conn.close()
            print("connection closed")