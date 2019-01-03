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

    job_args = ['-sbp_sample_id', str(args['sbp_sample_id']), '-verbose_cloud_sdk', '-use_preemtible_vms']

    for i in range(1, len(sys.argv)):
        job_args.append(sys.argv[i])

    spec = kubernetes.client.V1Job(
        metadata=kubernetes.client.V1ObjectMeta(
            name='pipelinev5-{0}-{1}'.format(args['sbp_sample_id'], timestamp)
        ),
        spec=kubernetes.client.V1JobSpec(
            completions=1,
            parallelism=1,
            template=kubernetes.client.V1PodTemplateSpec(
                spec=kubernetes.client.V1PodSpec(
                    restart_policy='Never',
                    containers=[
                        kubernetes.client.V1Container(
                            name='pipelinev5-{0}-{1}'.format(args['sbp_sample_id'], timestamp),
                            image='hartwigmedicalfoundation/bootstrap:{0}'.format(os.environ['PIPELINE_VERSION']),
                            command=[
                                '/bootstrap.sh'
                            ],
                            args=job_args,
                            env=[
                                kubernetes.client.V1EnvVar(
                                    name='BOTO_PATH',
                                    value='/mnt/boto-config/boto.cfg'
                                ),
                                kubernetes.client.V1EnvVar(
                                    name='READER_ACL_IDS',
                                    value='0403732075957f94c7baea5ad60b233f,f39de0aec3c8b5bb9d78a22ad88428ad'
                                ),
                                kubernetes.client.V1EnvVar(
                                    name='READER_ACP_ACL_IDS',
                                    value='0403732075957f94c7baea5ad60b233f'
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

    if len(samples) > 0:
        for sample in samples:
            start_kubernetes_job({'sbp_sample_id': sample.id})

            phone_home('Starting PipelineV5 for sample {0} barcode {1}'.format(sample.name, sample.barcode))

            sample.status = 'Started_PipelineV5'
            sample.save()
    else:
        log('No Pipeline v5 runs currently ready to go')


try:
    main()
except BaseException, e:
    message = "Exception in pipeline5 pending-check\n\n"
    message += str(e) + "\n\n"
    message += traceback.format_exc()
    message += "\nWill now sleep until human takes a look"

    phone_home(message)

    while True:
        time.sleep(60)
