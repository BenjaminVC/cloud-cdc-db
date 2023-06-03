import boto3
import json
import xml.etree.ElementTree as ET
import io
import os
import sys


# uploads valid xml or json file to CDC S3 bucket with tag
def upload_to_s3(s3_client, bucket, file_path, tag):
    print("Uploading to CDC...")
    try:
        # extract filename from file_path and upload file to S3
        file_name = os.path.basename(file_path)
        s3_client.upload_file(file_path, bucket, file_name)
        print("Uploading file...")

        # create waiter...
        waiter = s3_client.get_waiter('object_exists')
        # wait for upload to 'exist' before adding tag
        waiter.wait(Bucket=bucket, Key=file_name)

        # add tag to the uploaded file
        s3_client.put_object_tagging(
            Bucket=bucket,
            Key=file_name,
            Tagging={
                'TagSet': [
                    {
                        'Key': 'type',
                        'Value': tag
                    },
                ]
            }
        )
        print("Tagged", file_name, "with type =", tag)
        return True

    except Exception as e:
        print("Upload to CDC failed.")
        print(e)
        return False

# validates json file
def validate_json(file_path):
    with open(file_path, 'r') as json_file:
        try:
            data = json.load(json_file)
            # ensure the structure and fields of the json meets requirements
            if ('date' not in data or
                    'site' not in data or
                    'vaccines' not in data):
                print("Invalid JSON found in:", file_path)
                return False
            # ensure that for each vaccine data the structure is correct,
            # and that the firstShot and secondShot data adds up to the total
            for vaccine in data['vaccines']:
                if ('total' not in vaccine or
                        'firstShot' not in vaccine or
                        'secondShot' not in vaccine or
                        vaccine['total'] != vaccine['firstShot'] + vaccine['secondShot']):
                    print("Invalid vaccine data found in:", file_path)
                    return False
            return True

        except json.JSONDecodeError:
            print("Failed to parse JSON in", file_path)
            return False


# validates xml files
def validate_xml(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        # ensure the structure and fields of the xml meets requirements
        if (root.find('site') is None or
                root.find('vaccines') is None):
            print("Invalid XML found in", file_path)
            return False
        # ensure that for each vaccine data the structure is correct,
        # and that the firstShot and secondShot data adds up to the total
        for vaccine in root.findall('vaccines/brand'):
            if (vaccine.find('total') is None or
                    vaccine.find('firstShot') is None or
                    vaccine.find('secondShot') is None or
                    int(vaccine.find('total').text) != int(vaccine.find('firstShot').text) +
                    int(vaccine.find('secondShot').text)):
                print("Invalid vaccine data found in:", file_path)
                return False
        return True

    except ET.ParseError:
        print("Failed to parse XML in", file_path)
        return False


# checks if passed file exists, returns boolean
def check_dir(file_path):
    has_files = False

    # check for non-empty directory of vaccination data
    vaccine_data = file_path
    if not os.path.exists(vaccine_data):
        print("Could not find:", vaccine_data, "nothing to upload to CDC.")
    else:
        has_files = True
        print(vaccine_data, "has records to upload to CDC.")
    return has_files


# checks that the user passed two arguments
def has_args():
    num_args = len(sys.argv)
    if num_args >= 3:
        print("Valid # of arguments passed to UploadData.py")
        return True
    else:
        print("User did not pass a valid # of arguments to UploadData.py")
        return False


# check args to verify it the file extension matches the tag
def matches_filetype(file_path, tag):
    file_extension = file_path.split('.')[1]
    if file_extension == str(tag).lower():
        print("User passed file:", file_path, "and matching", tag, "tag")
        return True
    else:
        print("Tag did not match file type.")
        return False


# UploadData script start
if __name__ == '__main__':
    # initial setup vars
    can_upload = False

    # cli args
    valid_init = has_args()
    path_to_data = sys.argv[1]
    tag_arg = sys.argv[2]

    # check user passed file and that the tag_arg matches the file extension
    site_has_data = check_dir(path_to_data)
    tag_matches_extension = matches_filetype(path_to_data, tag_arg)

    # check conditions before creating aws client
    if site_has_data and valid_init and tag_matches_extension:
        if str(tag_arg).lower() == 'json' and validate_json(path_to_data):
            can_upload = True
        elif str(tag_arg).lower() == 'xml' and validate_xml(path_to_data):
            can_upload = True
        else:
            print("Could not validate", path_to_data)
            exit(2)
    else:
        print("Exiting...")
        exit(1)

    if can_upload:
        print("Ready to upload:", path_to_data)
        # boto3 s3 resource using default .aws credentials
        s3 = boto3.client('s3')
        bucket_name = 'cdc-vaccinationdrive'
        if upload_to_s3(s3, bucket_name, path_to_data, tag_arg):
            print("Uploaded to CDC S3 bucket:", bucket_name)
        else:
            print("File could not be uploaded to CDC S3 bucket:", bucket_name)

exit(0)
