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

# Updating the cronjob:

Authenticate to hmfp2 using the token app in Okta, then edit the cronjob:

```
kubectl -n acceptance edit cronjob pipelinev5-pending-check
```

And change the tag to your new image
