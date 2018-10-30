# Building a new image:

```
export TAG=v0.0.2

docker -t eu.gcr.io/hmf-pipeline-development/pipelinev5-pending-check:$TAG
```

Authenticate to GCP:

```
gcloud init
```

And push the image:

```
gcloud docker -- push eu.gcr.io/hmf-pipeline-development/pipelinev5-pending-check:$TAG
```
