# Shallow QC

Tool to extract QC information from a shallow pipeline5 run.
Output files:

- `<sample id>.shallow-qc.json`: JSON file containing QC information

## Background

TODO

## Build

Build the Docker image with

    docker build -t shallow-qc --platform linux/amd64 .

Run it for pipeline output in `~/data/pipeline-output` with:

    docker run \
      --platform linux/amd64 \
      -v ~/data:/data \
      -w /data/result \
      shallow-qc \
      <sample-id> /data/pipeline-output

In this example the output files will be written to the directory `~/data/result`.

## Release

Create a git tag like `shallow-qc-0.0.1` (so "shallow-qc-" followed by a semantic version).
Alpha and beta releases are also suppored (e.g. `shallow-qc-0.0.1-alpha.1`).
Push the tag to start a build.
