#!/bin/python

import smtplib
import os
import re
import fnmatch
import subprocess
import traceback
import datetime
import time
import requests
import json
import socket
import sys
import boto3
import csv
from xml.dom.minidom import parse
from StatsParser import StatsParser
from botocore.exceptions import ConnectionError

def log_txt(string):
    print('[' + datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f') + '] ' + string)
    with open("/data/conversionLog.txt", "a") as myfile:
        myfile.write('[' + datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f') + '] ' + string + "\n")


def phone_int(message):
    log_txt(message)

    payload = {"text": '```' + message + '```'}
    requests.post("https://hooks.slack.com/services/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", data=json.dumps(payload), headers={'Content-Type': 'application/json'})


def getPlatform():
    with open('/data/SampleSheet.csv', 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            row = [x.strip() for x in row]
            if row[0] == 'Application':
                return row[1]

    return False


def uploadToS3(file, bucket, object, retry=0):
    session = boto3.Session()
    endpoint, region = "https://s3.object02.schubergphilis.com", "eu-nl-prod01"
    s3 = session.client('s3',
                        endpoint_url=endpoint,
                        region_name=region
                        )
    # Bucket set above
    try:
        log_txt("Uploading " + file + " to " + object)
        s3.upload_file(file, bucket, object, ExtraArgs={'GrantRead': 'id=0403732075957f94c7baea5ad60b233f,id=6f794a6db112f27499a06697c125d7c4',
                                                        'GrantReadACP': 'id=0403732075957f94c7baea5ad60b233f'})
    except (ConnectionError) as e:
        if retry < 4:
            phone_int('Retrying upload failure from api-register file ' + str(file) + '\n' + str(e))
            time.sleep(120)
            uploadToS3(file, bucket, object, retry + 1)
        else:
            raise


def uploadStats(FCID):
    bucket = 'hmf-flowcell-stats-storage'
    for root, dirs, files in os.walk("/data/Data/Intensities/BaseCalls/Stats"):
        for name in files:
            uploadToS3(os.path.join(root, name), bucket, FCID + "/Stats/" + name)


def uploadReports(FCID):
    bucket = 'hmf-flowcell-stats-storage'
    for root, dirs, files in os.walk("/data/Data/Intensities/BaseCalls/Reports"):
        for name in files:
            uploadToS3(os.path.join(root, name), bucket, FCID + "/Reports/" + name)


def ConvertBcl():
    run_info = parse("/data/RunInfo.xml")
    FCID = run_info.getElementsByTagName('Flowcell')[0].firstChild.nodeValue

    log_txt("Flowcell ID in RunInfo is " + FCID)
    log_txt("Flowcell ID in ENVIRON is " + str('flowcell_id' in os.environ and os.environ['flowcell_id']))

    if 'flowcell_id' in os.environ and FCID.strip().upper() != os.environ['flowcell_id'].strip().upper():
        raise ValueError("Expected Flowcell ID did not match, PVC not clean ?")

    platform = getPlatform()

    log_txt("Detected platform: " + str(platform))
    log_txt("Running bcl2fastq")

    if platform == 'NextSeqFASTQOnly':
        p = subprocess.call("/usr/local/bin/bcl2fastq --runfolder-dir /data -r 1 -w 1 -p 12 > /data/conversionError.txt 2>&1", shell=True)
    else:
        p = subprocess.call("/usr/local/bin/bcl2fastq --runfolder-dir /data -r 6 -w 6 -p 12 > /data/conversionError.txt 2>&1", shell=True)

    log_txt("conversionError.txt contents")
    log_txt(open('/data/conversionError.txt').read())

    if p != 0 or 'Processing completed with 0 errors and' not in open('/data/conversionError.txt').read():
        raise ValueError('BCL conversion script threw an error or warning. Verify conversionError.txt')

    log_txt("Conversion done, starting rename")

    # ADD FLOWCELL ID TO FASTQ NAME
    for root, dirs, files in os.walk("/data/Data/Intensities/BaseCalls"):
        for name in files:
            if fnmatch.fnmatch(name, '*fastq.gz'):
                name_parts = name.split('_')
                if name_parts[1] != FCID:
                    newname = name.split('_')[0]+'_'+FCID+'_'+'_'.join(name.split('_')[1:])

                    if len(root.split('/')) < 7 and name_parts[0] != 'Undetermined':
                        if not os.path.isdir(os.path.join(root, name_parts[0])):
                            os.mkdir(os.path.join(root, name_parts[0]))
                        newname = name_parts[0] + '/' + newname

                    log_txt(name + " is about to be renamed to " + newname)
                    os.rename( os.path.join(root, name), os.path.join(root, newname) )
                else:
                    log_txt("Not renaming " + name)

    log_txt("Uploading Stats")
    uploadStats(FCID)
    log_txt("Uploading Reports")
    uploadReports(FCID)
    StatsParser()

    os.system("touch /data/conversionDoneSBP.txt")

def purge():
    for root, dirs, files in os.walk("/data/Data/Intensities/BaseCalls"):
        for name in files:
            if fnmatch.fnmatch(name, '*fastq.gz'):
                log_txt("deleting " + name)
                os.remove(os.path.join(root, name))


# ############ZE MAGIC#####################

log_txt("Starting BCL2FastQ Python script")

if os.path.isfile('/data/conversionDoneSBP.txt'):
    hn=""
    hn=socket.gethostname()
    phone_int('['+hn+'] Found previously completed run. Skipping conversion.')
    sys.exit(0)

try:

    if os.path.isfile('/data/SampleSheet.csv') and os.path.isfile('/data/RTAComplete.txt'):
        log_txt("Finding and removing any fastq files in the dir")
        purge()

        log_txt("Starting conversion")
        ConvertBcl()
    else:
        raise ValueError("Either SampleSheet or RTAComplete files are missing")

    log_txt("Finished BCL2FastQ Python script")
except BaseException, e:
    message = "Exception in bcl2fastq script\n\n"
    message += str(e) + "\n\n"
    message += traceback.format_exc()
    message += "\nWill now sleep until human takes a look"

    phone_int(message)

    while True:
        time.sleep(1)
