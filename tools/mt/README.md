# MT

Tool to extract mitochondrial DNA from a CRAM file into a separate BAM file. 
Output files:

- `<sample id>.mt.bam`: final sorted BAM
- `<sample id>.mt.bam.bai`: final sorted BAM index
- `<sample id>.mt.unsorted.bam`: unsorted BAM
- `<sample id>.readnames.txt`: list of all read names found

## Background

Mitochondrial DNA is not relevant for diagnostics, but is relevant for some researchers.

Redux version 1.1 and lower unmaps mitochondrial DNA. This is fixed in Redux 1.2 
(see [commit](https://github.com/hartwigmedical/hmftools/commit/7874f481ef690de68cbae8c7eb9384f3e621c1ed])).

This tool can be used to extract mitochondrial DNA from CRAMs created with Redux 1.1 and lower.

## Build

Build the Docker image with

    docker build -t mt --platform linux/amd64 .

Run it for a CRAM located in `~/data/file.cram` with:

    docker run \
      --platform linux/amd64 \
      -v ~/data:/data 
      mt \
      --cram /data/file.cram \
      --output-dir /data/result \
      --sample-id SAMPLE

In this example the output files will be copied to the directory `~/data/result`.

## Release

Create a git tag like `mt-0.0.1` (so "mt-" followed by a semantic version).
Push the tag to start a build.
