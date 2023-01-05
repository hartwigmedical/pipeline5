## What is this?

These are scripts and data for creating new base images for use in running PipelineV5 VMs on GCP. A base image is
the image used in VM creation during stage execution. In other words, it is the image used by 

## Usage

### Creating a public image

A public image contains all tools and resources with no licensing or confidentiality restriction. To create one
run the following from this directory.

```shell
./create_public_image.sh
```

The image will be named for the current version in development, along with a timestamp of creation. 

### Creating a public image with overrides

Creating a public image will use resources (static configuration of the pipeline like gene panels, reference genomes
etc) from the common-resources-public repository and common-resources bucket (large files). To override the files in
either of these locations, for instance to make an image for an external group, you can use the `--flavour` option.

First create a bucket of the conventions `gs://common-resources-overrides-{name}` where `name` is some identifier for the
custom image. Upload the files you wish to override into that bucket, making sure to match the path in the actual repo/bucket.

Then run `./create_public_image.sh --flavour {name}`

Here is an example of overriding the driver gene panel for a PMC image

```shell
gsutil cp /path/to/overridden/DriverGenePanel.38.tsv gs://common-resources-pmc-overrides/gene_panel/38/DriverGenePanel.38.tsv
./create_public_image.sh --flavour pmc
```

### Creating a public image with pilot jar

Images can also be created using local pilot versions of tools jars. Source image is a previously created public
image using `./create_public_image.sh`.

```shell
mkdir /path/to/pilot/jars/
cp sage_pilot.jar /path/to/pilot/jars/sage_pilot.jar
./create_pilot_image.sh -s ${source_image} -d /path/to/pilot/jars/
```

### Creating a private image

A private image overlays the contents of common-resources-private repository on a previously created public image.
These images are meant for production sequencing services.

```shell
./create_private_image.sh {source_image}
```

### Creating a custom docker container

The above scripts all deal in VM images, but in some cases (for instance to use pilot jars) we also need a custom 
docker container image. There is no script for this but it can be done by execution the following commands:

```shell

cd /path/to/root/of/pipeline5
mvn clean install -Prelease -DskipTests
cd cluster
docker build --build-arg VERSION=local-SNAPSHOT -t eu.gcr.io/hmf-build/pipeline5:{your_version} .
docker push eu.gcr.io/hmf-build/pipeline5:{your_version}
```

Where `your_version` is some descriptive name for the image, usually prefixed with the major version of the pipeline
(eg 5.31.pmc-1). The image can then be accessed from platinum or other docker based tools via the tag `eu.gcr.io/hmf-build/pipeline5:{your_version}`