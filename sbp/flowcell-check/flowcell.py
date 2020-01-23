#!/usr/bin/python

import sys
import traceback

import kubernetes

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

    job_args = ['-flowcell', str(args['flowcell']), '-sbp_api_url', 'http://hmfapi']

    for i in range(1, len(sys.argv)):
        job_args.append(sys.argv[i])

    spec = kubernetes.client.V1Job(
        metadata=kubernetes.client.V1ObjectMeta(
            name='bcl2fastq-flowcell-{0}-{1}'.format(args['flowcell'], timestamp).lower()
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
                            name='bcl2fastq-flowcell-{0}-{1}'.format(args['flowcell'], timestamp).lower(),
                            image='hartwigmedicalfoundation/bcl2fastq:{0}'.format(os.environ['PIPELINE_VERSION']),
                            command=[
                                '/bcl2fastq.sh'
                            ],
                            args=job_args,
                            volume_mounts=[
                                kubernetes.client.V1VolumeMount(
                                    name='gcp-bcl2fastq-scheduler',
                                    mount_path='/secrets/'
                                ),
                                kubernetes.client.V1VolumeMount(
                                    name='gcp-hmf-database',
                                    mount_path='/archive/'
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
                            name='gcp-bcl2fastq-scheduler',
                            secret=kubernetes.client.V1SecretVolumeSource(
                                secret_name='gcp-pipeline5-scheduler'
                            )
                        ),
                        kubernetes.client.V1Volume(
                            name='gcp-hmf-database',
                            secret=kubernetes.client.V1SecretVolumeSource(
                                secret_name='gcp-' + args['bucket'].replace('_', '-')
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
    flowcells = HmfApi().get_all(Flowcell, {'status': 'Pending'})
    max_starts = int(os.getenv('MAX_STARTS', '4'))

    if len(flowcells) > 0:
        log('Scheduling {0} out of {1} samples'.format(max_starts,len(flowcells)))
        del(flowcells[max_starts:])
        for flowcell in flowcells:

            start_kubernetes_job({'flowcell': flowcell.flowcell_id })

            phone_home('Starting bcl2fastq {0} for flowcell {1}'.format(os.environ['PIPELINE_VERSION'], flowcell.flowcell_id))

            flowcell.status = 'Conversion Started'
            flowcell.save()
    else:
        log('No flowcells currently ready to go')


try:
    main()
except BaseException, e:
    message = "Exception in bcl2fastq pending-check-flowcell\n\n"
    message += str(e) + "\n\n"
    message += traceback.format_exc()

    phone_home(message)
