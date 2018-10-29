#!/usr/bin/python

import json
import kubernetes
import os
import requests
import sys

from HmfApi import *

revision = str(sys.argv[1])

def phone_home(message):
    print message
    slack_hook = os.environ['SLACK_HOOK']
    payload = {"text": '```' + message + '```'}
    r = requests.post(slack_hook, data=json.dumps(payload), headers={'Content-Type': 'application/json'})

def start_kubernetes_job(type, args):
    kubernetes.config.load_incluster_config()

    spec = kubernetes.client.V1Job(
        metadata=kubernetes.client.V1ObjectMeta(
            name='scheduler-' + type + '-' + args[0],
            labels={
                'cleanup_job_name': 'scheduler-' + type
            }
        ),
        spec=kubernetes.client.V1JobSpec(
            completions=1,
            parallelism=1,
            template=kubernetes.client.V1PodTemplateSpec(
                spec=kubernetes.client.V1PodSpec(
                    restart_policy='Never',
                    containers=[
                        kubernetes.client.V1Container(
                            name='scheduler-' + type + '-' + args[0],
                            image='registry.saas.sbp.world/hmf/scheduler:' + revision,
                            command=[
                                'python',
                                '-u',
                                '/usr/src/app/schedule.py',
                                type,
                            ] + args
                        )
                    ]
                )
            )
        )
    )

    with file('/var/run/secrets/kubernetes.io/serviceaccount/namespace') as f:
        namespace = f.read()

    job = kubernetes. \
        client. \
        BatchV1Api(). \
        create_namespaced_job(
            namespace,
            spec
        )

    print ('Started job ' + job.metadata.name)

samples = HmfApi().get_all(Sample, {'status': 'Pending_PipelineV5'})
if len(samples) > 0:
    for sample in samples:
        start_kubernetes_job('bootstrap', [str(sample.id)])
        phone_home("Starting PipelineV5 for sample  " + str(sample.name))
        sample.status = 'Started_PipelineV5'
        sample.save()
else:
    print "No Pipeline v5 runs currently ready to go"
