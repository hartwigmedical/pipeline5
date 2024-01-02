# What is this directory?

These are scripts and data for creating new disk images for running PipelineV5 VMs on GCP. The image must include all of the tools
that are required by the pipeline version being executed, and must be functional standalone OS. The images we create are based on
the Debian image provided by Google.

Resources and tools are copied in from buckets and other sources at imaging time. The image is created with a name and "family"
that make it clear which version of the pipeline it should work with. The contract between the image and the pipeline code version
is quite loose so production pipeline builds specify the exact image name in their arguments. Without this argument the latest
image in the same family as the executing pipeline will be used, which is convenient for development.

As of December 2023 migration has begun to a situation in which only externally-maintained tools should be pulled from the
`common-tools` bucket, HMF tools should be pushed to Artifact Registry via tags and retrieved from there as well. See the
`VersionUtils` and `HmfTool` classes in the `pipeline5` code to see how to make this happen for your dependency.

# Creating a public image

A public image contains all tools and resources with no licensing or confidentiality restriction. The developer will need to make
one of these when a new tool or version of an existing tool is added. To create one run from this directory:

```shell
./create_public_image.sh
```

The image will be named for the current version in development, along with a timestamp of creation.

This image will be appropriate for development testing but production will need a private image, see below.

# Creating a public image with overrides

Creating a public image will use resources (static configuration of the pipeline like gene panels, reference genomes
etc) from the common-resources-public repository and common-resources bucket (large files). To override the files in
either of these locations, for instance to make an image for an external group, you can use the `--flavour` option.

First create a bucket of the conventions `gs://common-resources-{name}-overrides` where `name` is some identifier for the
custom image. Upload the files you wish to override into that bucket, making sure to match the path in the actual repo/bucket.

Then run `./create_public_image.sh --flavour {name}`

Here is an example of overriding the driver gene panel for a PMC image

```shell
gsutil cp /path/to/overridden/DriverGenePanel.38.tsv gs://common-resources-pmc-overrides/gene_panel/38/DriverGenePanel.38.tsv
./create_public_image.sh --flavour pmc
```

# Creating a public image with pilot JAR

Images can also be created using local pilot versions of tools JARs. The source image must be a previously-created public
image as created by `./create_public_image.sh`.

```shell
mkdir /path/to/pilot/jars/
cp sage_pilot.jar /path/to/pilot/jars/sage.jar
./create_pilot_image.sh -s ${source_image} -d /path/to/pilot/jars/
```

The resulting image will be created very quickly because it uses the public image as a base and overlays the pilot JARs, but is
not designed for regular use. It can be utilised by passing the arguments `-image_project hmf-pipeline-development 
-image_name {name_from_create_script}` to the Pipeline5 application, it will not have the same family as regular images so won't 
be auto-located.

In addition, `Hmftool.java` contains an enum with flags to enable pilot mode. Set the flag to true for the tool being tested before 
compiling/running the Pipeline5 application with the above arguments.

# Creating a private image

A private image overlays the contents of the `common-resources-private` repository on a previously created public image.
These images are meant for production sequencing services.

```shell
./create_private_image.sh {source_image}
```

# Creating a custom Docker container

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
