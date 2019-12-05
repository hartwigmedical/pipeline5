#!/usr/bin/python

import csv
import os
import boto3
import traceback
import time
import hashlib
import json
from HmfApi import *
from botocore.exceptions import ConnectionError

if os.getenv('HMF_ENVIRONMENT') == 'dev':
    info_path = 'samples/'
    data_path = 'samples/BaseCalls/'
    devmode = True
else:
    info_path = '/data/'
    data_path = '/data/Data/Intensities/BaseCalls/'
    devmode = False


def phone_ext(message):
    print message

    if devmode is not True:
        payload = {"text": '```' + message + '```'}
        requests.post("https://hooks.slack.com/services/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", data=json.dumps(payload), headers={'Content-Type': 'application/json'})


def phone_int(message):
    print message

    if devmode is not True:
        payload = {"text": '```' + message + '```'}
        requests.post("https://hooks.slack.com/services/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", data=json.dumps(payload), headers={'Content-Type': 'application/json'})


def get_md5_sum(file):
    m = hashlib.md5()
    with open(file, "rb") as f:
        while True:
            buf = f.read(2**20)
            if not buf:
                break
            m.update(buf)
    return m.hexdigest()


def parseSampleSheet():
    flowcell = Flowcell()
    samples = {}
    lanes = {}

    with open(info_path + 'SampleSheet.csv', 'rb') as f:
        reader = csv.reader(f)
        data_section = False
        data_headers = False

        for row in reader:
            row = [x.strip() for x in row]

            if len(row) == 0:
                continue

            if row[0] == 'ExperimentName':
                flowcell.get_one({'name': row[1]})

                if not hasattr(flowcell, 'flowcell_id') or flowcell.flowcell_id is None:
                    raise ValueError("FATAL: Flowcell not found in database")
                continue

            if row[0] == '[Data]':
                data_section = True
                continue

            if row[1] == '' and row[2] == '':
                continue

            if data_section and not data_headers:
                data_headers = [x.upper() for x in row]

                lane = sample_id = sample_name = sample_project = False

                if 'LANE' in data_headers:
                    lane = data_headers.index('LANE')

                if 'SAMPLE_ID' in data_headers:
                    sample_id = data_headers.index('SAMPLE_ID')

                if 'SAMPLE_NAME' in data_headers:
                    sample_name = data_headers.index('SAMPLE_NAME')

                if 'SAMPLE_PROJECT' in data_headers:
                    sample_project = data_headers.index('SAMPLE_PROJECT')

                if not all(map(lambda x:x is not False, [sample_id, sample_name, sample_project])):
                    raise ValueError('FATAL: Missing header in SampleSheet ' + str(data_headers) + str([sample_id, sample_name, sample_project]))

                if lane is False:
                    # No lanes defined, try the Stats.json from bcl2fastq
                    try:
                        with open(os.path.join(data_path, 'Stats', 'Stats.json')) as f:
                            lanes_nrs = [str(x['LaneNumber']) for x in json.load(f)['ReadInfosForLanes']]
                    except BaseException, e:
                        # No Lanes defined unfortunately, adding four as best guess
                        lanes_nrs = ['1', '2', '3', '4']
                        pass

                    for id in lanes_nrs:
                        if 'L00' + str(id) not in lanes:
                            l = Lane()
                            l.fastq_count = 0

                            if l.get_one({'flowcell_id': flowcell.id, 'name': 'L00' + str(id)}) is False:
                                l.name = 'L00' + str(id)
                                l.flowcell_id = flowcell.id
                                l.save()

                            lanes['L00' + str(id)] = l

                continue

            if data_section and data_headers:
                if lane is not False:
                    if 'L00' + row[lane] not in lanes:
                        l = Lane()
                        l.fastq_count = 0

                        if l.get_one({'flowcell_id': flowcell.id, 'name': 'L00' + row[lane]}) is False:
                            l.name        = 'L00' + row[lane]
                            l.flowcell_id = flowcell.id
                            l.save()

                        lanes['L00' + row[lane]] = l

                if row[sample_id] not in samples:
                    s = Sample()

                    if s.get_one({'barcode': row[sample_id]}) is False:
                        s.barcode     = row[sample_id]
                        s.submission  = row[sample_project]
                        s.status      = 'Unregistered'
                    elif s.submission != row[sample_project]:
                        raise ValueError("FATAL: Sample submission for " + str(s.barcode) + " does not match registration (" + str(s.submission) + " / " + str(row[sample_project]) + "). Correct the SampleSheet or registration")

                    s.name        = row[sample_name]
                    s.lanes       = []
                    s.save()
                    samples[row[sample_id]] = s

                if lane is False:
                    for id in lanes_nrs:
                        samples[row[sample_id]].lanes.append('L00' + id)
                else:
                    samples[row[sample_id]].lanes.append('L00' + row[lane])

                continue

    return flowcell, samples, lanes


def getFastQ(flowcell, samples, lanes):
    for sample_id, sample in samples.iteritems():
        for l in sample.lanes:
            lane = lanes[l]

            f = FastQ()

            if f.get_one({'lane_id': lane.id, 'sample_id': sample.id}) is False:
                f.lane_id   = lane.id
                f.sample_id = sample.id

            fastqs_dir = data_path + sample.submission + '/' + sample.barcode

            if os.path.isdir(fastqs_dir):
                for file in os.listdir(fastqs_dir):
                    if sample.name + '_' in file and '_' + flowcell.flowcell_id + '_' in file and '_' + l + '_' in file:
                        if '_R1_' in file:
                            f.name_r1 = file
                            f.size_r1 = os.path.getsize(fastqs_dir + '/' + file)
                        if '_R2_' in file:
                            f.name_r2 = file
                            f.size_r2 = os.path.getsize(fastqs_dir + '/' + file)

                if f.name_r1 is not None or f.name_r2 is not None:
                    lane.fastq_count += 1
                    f.save()
            else:
                print "Error: " + fastqs_dir + " does not exist"


def uploadToS3(file, bucket, object, retry=0):
    session = boto3.Session()
    endpoint, region = "https://s3.object02.schubergphilis.com", "eu-nl-prod01"
    s3 = session.client('s3',
                        endpoint_url=endpoint,
                        region_name=region
                        )
    # Bucket set above
    try:
        s3.upload_file(file, bucket, object, ExtraArgs={'GrantRead': 'id=0403732075957f94c7baea5ad60b233f,id=6f794a6db112f27499a06697c125d7c4',
                                                        'GrantReadACP': 'id=0403732075957f94c7baea5ad60b233f'})
    except (ConnectionError) as e:
        if retry < 4:
            phone_int('Retrying upload failure from api-register file ' + str(file) + '\n' + str(e))
            time.sleep(120)
            uploadToS3(file, bucket, object, retry + 1)
        else:
            raise


def parseFastqStats(flowcell, samples, lanes):
    with open(info_path + 'fastq-stats.csv', 'rb') as f:
        reader = csv.reader(f)
        samples_on_flowcell_qc = True

        for row in reader:
            if row[0].strip() == 'Flowcell':
                flowcell_name, flowcell_yield, flowcell_q30 = row[1].strip(), row[2].strip(), row[3].strip()

                flowcell.q30 = flowcell_q30
                flowcell.yld = flowcell_yield
                flowcell.save()

            if row[0].strip() == 'Lane':
                lane_name, lane_yield, lane_q30 = row[1].strip(), row[2].strip(), row[3].strip()

                lane          = lanes[lane_name]
                lane.q30      = lane_q30
                lane.q30_pass = False if float(lane_q30) < 75.0 else True
                lane.yld      = lane_yield
                lane.yld_pass = False if int(lane_yield) < 100000000000 else True
                lane.save()

            if row[0].strip() == 'Sample':
                sample_name, sample_yield, sample_q30 = row[1], row[2], row[3]

                if sample_name.strip() == 'Undetermined':
                    flowcell.undet_rds        = sample_yield
                    flowcell.undet_rds_p      = float(sample_yield) / float(flowcell_yield) * 10000

                    if flowcell.undet_rds_p > 600 or not samples_on_flowcell_qc:
                        flowcell.undet_rds_p_pass = False
                    else:
                        flowcell.undet_rds_p_pass = True

                    flowcell.save()
                elif int(sample_yield) < 1000000000:
                    samples_on_flowcell_qc = False

            if row[0].strip() in samples:
                sample_name, lane_name, fastq_yield, fastq_q30 = row[0].strip(), row[1].strip(), row[2].strip(), row[3].strip()

                if int(fastq_yield) == 0 and int(fastq_q30) == 0:
                    continue

                sample = samples[sample_name]
                lane   = lanes[lane_name]
                f      = FastQ()

                fastqs_dir = data_path + sample.submission + '/' + sample.barcode + '/'

                if f.get_one({'sample_id': sample.id, 'lane_id': lane.id}) is False:
                    raise ValueError("FastQ not found for sample " + str(sample.id) + " with lane " + str(lane.id))

                f.q30    = fastq_q30
                f.yld    = fastq_yield
                f.bucket = 'hmf-fastq-storage'

                if sample.submission not in ['HMFregCPCT', 'HMFregDRUP', 'HMFregWIDE', 'HMFregGIAB', 'ILLUMINA']:
                    if f.name_r1:
                        f.hash_r1 = get_md5_sum(fastqs_dir + f.name_r1)
                    if f.name_r2:
                        f.hash_r2 = get_md5_sum(fastqs_dir + f.name_r2)

                f.save()

                try:
                    namespace = open('/var/run/secrets/kubernetes.io/serviceaccount/namespace','r').read()
                    if namespace == 'acceptance':
                        bucket = 'hmf-fastq-acc-storage'
                    elif namespace == 'production':
                        bucket = 'hmf-fastq-storage'
                except IOError:
                    # Probably not running in a kubernetes cluster
                    if devmode is not True:
                        raise ValueError('Cannot read kubernetes namespace and devmode is not set')
                    else:
                        pass

                if devmode is not True:
                    if f.name_r1:
                        uploadToS3(fastqs_dir + f.name_r1, bucket, f.name_r1)
                    if f.name_r2:
                        uploadToS3(fastqs_dir + f.name_r2, bucket, f.name_r2)

    for sample_id, sample in samples.iteritems():
        fastqs = HmfApi().get_all(FastQ, {'sample_id': sample.id})

        # Refresh the sample - could have changed since we started
        if sample.get(sample.id) is False:
            raise ValueError("Error: Failed to refresh sample " + str(sample.id))

        sample.yld = 0
        sample.q30 = 0
        sample.possible_yld = 0

        if len(fastqs) > 0:
            for f in fastqs:
                if f.bucket is None:
                    continue

                lane = Lane()
                flowcell = Flowcell()

                if lane.get(f.lane_id) is False or flowcell.get(lane.flowcell_id) is False:
                    raise ValueError("Error: Lane " + str(f.lane_id) + " or Flowcell " + str(lane.flowcell_id) + " not found in database")

                if f.yld is not None:
                    sample.possible_yld += f.yld
                else:
                    raise ValueError("Error: Fastq yield for fastq " + str(f.id) + " is None")

                if sample.q30_req is not None and f.q30 >= sample.q30_req and flowcell.undet_rds_p_pass is True:
                    sample.yld += f.yld
                    sample.q30 += f.q30 * f.yld

                    if f.qc_pass is False:
                        f.qc_pass = True
                        f.save()

            if sample.yld > 0:
                sample.q30 = sample.q30 / sample.yld

        if sample.status == 'Ready':
            phone_ext(
                'Sample %s is Ready but got additional data from flowcell %s. '
                'Please verify with HMF how to proceed.' % (sample.barcode, flowcell.flowcell_id)
            )

        if sample.yld_req is not None and sample.q30_req is not None:
            if sample.yld < sample.yld_req or sample.q30 < sample.q30_req:
                sample.status = 'Insufficient Quality'
            else:
                sample.status = 'Ready'
        elif sample.status == 'Sequencing':
                sample.status = 'Unregistered'

        sample.save()
        sample.set_to_none('version')


def report(flowcell, samples, lanes):
    output = (
        "              Flowcell report\n"
        "================================================\n"
        "                Name:  " + str(flowcell.name) + "\n"
        "                  ID:  " + str(flowcell.flowcell_id) + "\n"
        "              Status:  " + str(flowcell.status) + "\n"
        "                 Q30:  " + str(flowcell.q30) + "\n"
        "               Yield:  " + str(flowcell.yld) + "\n"
        "  Undetermined Reads:  " + str(round(flowcell.undet_rds_p / 100, 2)) + "%\n"
        "             QC Pass:  " + str("Yes" if flowcell.undet_rds_p_pass else "No") + "\n"
        "\n"
        "Lane   Q30       Yield          Quality\n"
    )

    for a, l in lanes.iteritems():
        output += '{0: <7}'.format(l.name) + \
                  '{0: <10}'.format("%.4f" % float(l.q30)) + \
                  '{0: <15}'.format(l.yld) + \
                  '{0: <9}'.format("OK" if l.q30_pass and l.yld_pass else "FAIL") + \
                  ("No FastQ data found" if l.fastq_count == 0 else "") + \
                  "\n"

    output += "\nBarcode      Name           Submission   Type    Status        Q30      Yield(QC PASS)  All Yield\n"

    for a, s in samples.iteritems():
        output += '{0: <13}'.format(s.barcode) + \
                  '{0: <15}'.format(s.name) + \
                  '{0: <13}'.format(s.submission) + \
                  '{0: <8}'.format(s.type) + \
                  '{0: <14}'.format('Insuf. QC' if s.status == 'Insufficient Quality' else s.status) + \
                  '{0: <9}'.format("%.4f" % float(s.q30)) + \
                  '{0: <16}'.format(s.yld) + \
                  '{0: <14}'.format(s.possible_yld) + \
                  "\n"

    phone_ext(output)

try:
    if not os.path.exists(info_path + 'SampleSheet.csv'):
        raise ValueError('Sample Sheet is missing for BCL Conversion')

    if not os.path.exists(info_path + 'fastq-stats.csv'):
        raise ValueError('fastq stats are missing for BCL Conversion')

    flowcell, samples, lanes = parseSampleSheet()
    getFastQ(flowcell, samples, lanes)
    parseFastqStats(flowcell, samples, lanes)

    flowcell.status = 'Converted'
    flowcell.save()

    flowcell.get(flowcell.id)
    flowcell.convertTime = flowcell.updateTime
    flowcell.save()

    report(flowcell, samples, lanes)
except BaseException, e:
    message = "Exception in api-register script\n\n"
    message += str(e) + "\n\n"
    message += traceback.format_exc()
    message += "\nWill now sleep until human takes a look"

    phone_int(message)

    while devmode is not True:
        time.sleep(1)
