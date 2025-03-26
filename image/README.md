# What is this directory?

Tools for creating new disk images for running `pipeline5` VMs on GCP. The `pipeline5` application consists of a Java application
that is packaged into a Docker container which implements a purpose-built workflow engine. The stages of the workflow are run on
VMs using a disk image compatible with the version of the Java application. The image must include all the tools that are
required by all stages of the pipeline, and must be a functional standalone OS. The images we create are based on the
Debian image provided by Google with custom additions:

* Our tools or their dependencies use Python, R and Perl. As of the update to Debian 12 these are managed via `anaconda`. Other
  dependencies are managed using the Debian package manager.
* Resources are layered in from both cloud storage buckets (for very large files) and cloud source repositories.
* Both HMF-maintained and external tools are copied in from a cloud storage bucket. Additionally, `Artifact Registry` may be used
  as a source, see below.

The image is created with a name and "family" that make it clear which version of the pipeline it should work with. The contract
between the image and the pipeline code version is quite loose so production pipeline builds specify the exact image name in their
arguments. Without this argument the latest image in the same family as the executing pipeline will be used, which is convenient
for development.

As of December 2023 migration has begun to a situation in which only externally-maintained tools should be pulled from the
`common-tools` bucket, HMF tools should be pushed to Artifact Registry via tags and retrieved from there as well. See the
`VersionUtils` and `HmfTool` classes in the `pipeline5` code to see how to make this happen for your dependency.

# Quickstart

Usually these scripts are used to update the image for a new version of `pipeline5`. The steps in this case are:

1. Update the Java application code to reference new versions of the tools in the stages and if necessary change anything else
2. Ensure new versions of tools are available in `common-tools`, ie `gsutil ls gs://common-tools/**` should show your JAR in the
   correct location, namespaced under its version
3. Update any resources in the `common-resources` bucket, public repository or private repository
4. Run the `create_public_image.sh` script
5. Run the `create_private_image.sh` script passing the name of the public image created in the previous step
6. Update references to the image in your launch configurations

# Details

More on each step from the quickstart and special uses.

## Creating a public image

A public image contains all tools and resources with no licensing or confidentiality restriction. The developer will need to make
one of these when a new tool or version of an existing tool is added. To create one, run from this directory:

```shell
./create_public_image.sh
```

The image will be named for the current version in development, along with a timestamp of creation. The "family" of the image is
set and the pipeline uses the latest image in a family when a specific image is not given, which is convenient for development.
The image should be manually copied to production and the launcher updated with the particular image name when the release is
rolled out.

## Creating a public image with overrides

Creating a public image will use resources (static configuration of the pipeline like gene panels, reference genomes
etc.) from the common-resources-public repository and common-resources bucket (large files). To override the files in
either of these locations, for instance to make an image for an external group, you can use the `--flavour` option.

First create a bucket of the conventions `gs://common-resources-{name}-overrides` where `name` is some identifier for the
custom image. Upload the files you wish to override into that bucket, making sure to match the path in the actual repo/bucket.

Then run `./create_public_image.sh --flavour {name}`

Here is an example of overriding the driver gene panel for a PMC image

```shell
gsutil cp /path/to/overridden/DriverGenePanel.38.tsv gs://common-resources-pmc-overrides/gene_panel/38/DriverGenePanel.38.tsv
./create_public_image.sh --flavour pmc
```

## Creating a public image with pilot JAR

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

## Creating a custom Docker container

The above scripts all deal in VM images, but in some cases (for instance to use pilot jars) we also need a custom 
docker container image. There is no script for this but it can be done by execution the following commands:

```shell

cd /path/to/root/of/pipeline5
mvn clean install -Prelease -DskipTests
cd cluster
docker build --build-arg VERSION=local-SNAPSHOT -t europe-west4-docker.pkg.dev/hmf-build/hmf-docker/pipeline5:{your_version} .
docker push europe-west4-docker.pkg.dev/hmf-build/hmf-docker/pipeline5:{your_version}
```

Where `your_version` is some descriptive name for the image, usually prefixed with the major version of the pipeline
(eg 5.31.pmc-1). The image can then be accessed from platinum or other docker based tools via the tag `europe-west4-docker.pkg.dev/hmf-build/hmf-docker/pipeline5:{your_version}`

## Maintenance

Hints on what to do when this image has to change.

### OS Update

Essentially this is starting from scratch with the new version. General ideas:

* Modify the source image in the main `create_public_image.sh` script and try to run it in-place. Most likely won't work.
* Change any custom repository URLs, etc. to align with the new release.
* All changes from the Google-supplied image need to be captured in these scripts so they can be run every time from scratch.

### R/Perl/Python Dependencies

Manually create a new VM from the latest functional image. The Anaconda-created environment is in `/opt/tools/anaconda3` and can be
enabled using a few activation commands. Look at the `pipeline5` Java code to see how this is done by the application.

The `anaconda.yaml` file here can be used as a reference or to start from scratch (eg using `conda env create -f ...`)  and should
also be updated when you are finished. This file is not used in the regular imaging flow but is used any time the Anaconda
environment needs to be modified. Keep it up to date by running a `conda env export` when you are finished.

When the environment is functioning as expected it can be bundled into a compressed TAR and the existing file in the tools bucket
backed up before the new tarball is copied overtop it. Be mindful, if your changes are not backward-compatible the new file should
be copied with a new name so that imaging runs on existing production versions continue to work if needed.

