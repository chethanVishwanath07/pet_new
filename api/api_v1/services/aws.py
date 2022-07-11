import boto3, logging
from botocore.exceptions import ClientError
from ..config import get_settings
from fastapi import HTTPException
from pdf2image import convert_from_bytes
import io
import pandas as pd
import pdfkit as pdf
import os



def getDataframeToPdfBase64(file_name , df ):
    name, format = get_file_format(file_name)
    df.to_html('{0}.html'.format(name))
    pdf.from_file('{0}.html'.format(name), '{0}.pdf'.format(name))
    #bas64pdf = pdf.read()
    #f = open("{0}.pdf".format(name), "r")
    #base_64 = f.read()
    f = open("{0}.pdf".format(name), "rb")
    base_64 = f.read().decode(errors='replace')

    #print(f.read())
    os.remove('{0}.html'.format(name))
    os.remove('{0}.pdf'.format(name))
    return base_64, name


def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=get_settings().AWS_KEY,  # 'AKIA4EQK62742QAW2273',
        aws_secret_access_key=get_settings().AWS_SECRET,  # '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',
        region_name=get_settings().AWS_REGION)


def downloadfile_from_s3_bucket(url: str):
    s3_client = get_s3_client()
    try:
        url = s3_client.generate_presigned_url('get_object',
                                               Params={'Bucket': get_settings().AWS_BUCKET_NAME, 'Key': url},
                                               HttpMethod="GET",
                                               ExpiresIn=6000)  # this url will be available for 6000 seconds
        return url


    except ClientError as err:
        logging.info("INFO: Failed to  dowload file {} from s3".format(url))
        logging.error(err)
        raise HTTPException(status_code=err.response['ResponseMetadata']['HTTPStatusCode'],
                            detail=err.response['Error']['Message'])

def get_file_format(file_name):

    file_name_split = file_name.split(".")
    if len(file_name_split) > 0:
        return file_name_split[0],file_name_split[1]



async def upload_file(file_name, file):

    try:
       # file_format = get_file_format(file_name)

        # Creating Session With Boto3.
        session = boto3.Session(
            aws_access_key_id=get_settings().AWS_KEY,
            aws_secret_access_key=get_settings().AWS_SECRET,
            region_name=get_settings().AWS_REGION
        )
        # Creating S3 Resource From the Session.
        s3 = session.resource('s3')

        # Aws bucket name
        object = s3.Object(
            get_settings().AWS_BUCKET_NAME,
            file_name,

        )
        # if (file_format == ".pdf"):
        object.put(Body=file, ContentType='application/pdf')
        # else :
        #     object.put(Body=file)

        return "File Uploaded"
    except Exception as err:
        print("Error", err)
        return "INVALID_FILE"

async def get_file(file_name):
    try:
        s3 = boto3.resource('s3', region_name=get_settings().AWS_REGION, aws_access_key_id=get_settings().AWS_KEY,
                        aws_secret_access_key=get_settings().AWS_SECRET)

        obj = s3.Object(get_settings().AWS_BUCKET_NAME, file_name)
        data = obj.get()['Body'].read()

        return data

    except Exception as err:
        print("Error", err)
        return "INVALID_FILE"

def text_extract(file, feature_types):
    print("In aws.py")
    try:
        Settings = get_settings()
        images = convert_from_bytes(
            file,
            grayscale=True,
            fmt="jpg",
            dpi=600,
        )
        # Creating Session With Boto3
        client = boto3.client(
            'textract',
            aws_access_key_id=Settings.AWS_KEY,
            aws_secret_access_key=Settings.AWS_SECRET,
            region_name="ap-south-1"
        )

        responselist = []
        for page_num, image in enumerate(images):
            print(page_num)
            # save the image as byte array, to be given to textract as input
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format='JPEG')
            img_byte_arr = img_byte_arr.getvalue()

            response = client.analyze_document(
                Document={
                    'Bytes': img_byte_arr,
                },
                FeatureTypes=feature_types
            )
            responselist.append(response)


        return responselist

    except Exception as err:
        print("Error in text extract", err)
        return "INVALID_FILE"

