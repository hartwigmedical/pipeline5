#!/usr/bin/python

import json
import kubernetes
import os
import requests
import sys
import time
import traceback

from HmfApi import *


def phone_home(message):
    print message

    payload = {"text": '```' + message + '```'}
    requests.post(
        os.environ['SLACK_HOOK'],
        data=json.dumps(payload),
        headers={'Content-Type': 'application/json'}
    )


def log(msg):
    print (time.strftime('[%Y-%m-%d %H:%M:%S] ') + str(msg))


def start_kubernetes_job(args):
    kubernetes.config.load_incluster_config()

    timestamp = time.strftime('%Y%m%d%H%M%S')
    with file('/var/run/secrets/kubernetes.io/serviceaccount/namespace') as f:
        namespace = f.read()

    job_args = ['-sbp_sample_id', str(args['sbp_sample_id'])]

    for i in range(1, len(sys.argv)):
        job_args.append(sys.argv[i])

    spec = kubernetes.client.V1Job(
        metadata=kubernetes.client.V1ObjectMeta(
            name='pipelinev5-sample-{0}-{1}'.format(args['sbp_sample_id'], timestamp)
        ),
        spec=kubernetes.client.V1JobSpec(
            completions=1,
            parallelism=1,
            backoff_limit=6,
            template=kubernetes.client.V1PodTemplateSpec(
                spec=kubernetes.client.V1PodSpec(
                    restart_policy='Never',
                    containers=[
                        kubernetes.client.V1Container(
                            name='pipelinev5-sample-{0}-{1}'.format(args['sbp_sample_id'], timestamp),
                            image='hartwigmedicalfoundation/pipeline5:{0}'.format(os.environ['PIPELINE_VERSION']),
                            command=[
                                '/pipeline5.sh'
                            ],
                            args=job_args,
                            env=[
                                kubernetes.client.V1EnvVar(
                                    name='READER_ACL_IDS',
                                    value='6f794a6db112f27499a06697c125d7c4,f39de0aec3c8b5bb9d78a22ad88428ad'
                                ),
                                kubernetes.client.V1EnvVar(
                                    name='READER_ACP_ACL_IDS',
                                    value='f39de0aec3c8b5bb9d78a22ad88428ad'
                                ),
                                kubernetes.client.V1EnvVar(
                                    name='BOTO_PATH',
                                    value='/mnt/boto-config/boto.cfg'
                                )
                            ],
                            volume_mounts=[
                                kubernetes.client.V1VolumeMount(
                                    name='hmf-upload-credentials',
                                    mount_path='/root/.aws'
                                ),
                                kubernetes.client.V1VolumeMount(
                                    name='gcp-pipeline5-scheduler',
                                    mount_path='/secrets/'
                                ),
                                kubernetes.client.V1VolumeMount(
                                    name='rclone-config',
                                    mount_path='/root/.config/rclone'
                                ),
                                kubernetes.client.V1VolumeMount(
                                    name='boto-config',
                                    mount_path='/mnt/boto-config'
                                )
                            ],
                            resources=kubernetes.client.V1ResourceRequirements(
                                requests={
                                    'memory': '2Gi'
                                }
                            )
                        )
                    ],
                    volumes=[
                        kubernetes.client.V1Volume(
                            name='hmf-upload-credentials',
                            secret=kubernetes.client.V1SecretVolumeSource(
                                secret_name='hmf-upload-credentials'
                            )
                        ),
                        kubernetes.client.V1Volume(
                            name='gcp-pipeline5-scheduler',
                            secret=kubernetes.client.V1SecretVolumeSource(
                                secret_name='gcp-pipeline5-scheduler'
                            )
                        ),
                        kubernetes.client.V1Volume(
                            name='rclone-config',
                            secret=kubernetes.client.V1SecretVolumeSource(
                                secret_name='rclone-config'
                            )
                        ),
                        kubernetes.client.V1Volume(
                            name='boto-config',
                            config_map=kubernetes.client.V1ConfigMapVolumeSource(
                                name='boto-config'
                            )
                        )
                    ]
                )
            )
        )
    )

    job = kubernetes. \
        client. \
        BatchV1Api(). \
        create_namespaced_job(
            namespace,
            spec
        )

    log('Started job {0}'.format(job.metadata.name))
    return job


def main():
    samples = HmfApi().get_all(Sample, {'status': 'Pending_PipelineV5'})
    max_starts = int(os.getenv('MAX_STARTS', '4'))

    if len(samples) > 0:
        log('Scheduling {0} out of {1} samples'.format(max_starts,len(samples)))
        del(samples[max_starts:])
        for sample in samples:
            start_kubernetes_job({'sbp_sample_id': sample.id})

            phone_home('Starting Pipeline {0} for sample {1} barcode {2}'.format(os.environ['PIPELINE_VERSION'], sample.name, sample.barcode))

            sample.status = 'Started_PipelineV5'
            sample.version = str(os.environ['PIPELINE_VERSION'])
            sample.save()
    else:
        log('No Pipeline v5 samples currently ready to go')


try:
    main()
except BaseException, e:
    message = "Exception in pipeline5 pending-check-sample\n\n"
    message += str(e) + "\n\n"
    message += traceback.format_exc()

    phone_home(message)
