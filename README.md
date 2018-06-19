# HMF Pipeline2
Pipeline2 is a new data pipeline build to perform secondary and tertiary analysis on patient DNA samples. Pipeline2 is currently in an
MVP phase to evaluate the GATK4 and ADAM frameworks. Two pipelines are implemented that take multiple lanes of paired-end FASTQ files as
input and return an aligned, sorted and duplicate marked BAM.

## Building and Testing Pipeline2

The only pre-requisite for running the build is having bwa installed locally (for the ADAM functional tests). For Mac users bwa can be
installed easily via homebrew, and there are also packages for most linux distributions. All else fails you can build from source
http://bio-bwa.sourceforge.net/

Pipeline2 is built with maven. To run all tests and also build the Docker image run the following:

```
mvn clean install
```
Building the Docker image is quite time consuming and probably not necessary for most testing (see next section). To disable the building
of the Docker image run the following.
```
mvn clean install -DskipDocker
```

## Running Pipeline2 locally

### Configuration

Pipeline2 expects a yaml file **conf/pipeline.yaml** relative to the processes working directory. See **system/docker/conf/pipeline.yaml**
for an example of this file. Within the yaml file you can configure the following:

| Parameter               | Description
| ----------------------- | ---------------------------------------------
| **pipeline**            | Parameters which impact the running of the pipeline
| flavour                 | Which test pipeline to use ADAM or GATK
| **spark**               | Parameters specific to Apache Spark
| master                  | The spark master user (ie local[#cpus], yarn, spark url, etc)
| **patient**             | Parameters to configure the patient and reference data
| name                    | Name of the patient with no sample type postfix
| directory               | Directory of patient FASTQ files.
| referencePath           | Full path to reference genome FASTA file

The simplest way to run the pipeline locally is to simply run PipelineRuntime main class from IntelliJ. The working directory will be the
root of the project so just add your /conf directory there as well.

## Running Pipeline2 with Docker

### Locally
You can also run locally as a Docker container. First build the project without the **-DskipDocker** flag to build the image. It will be
tagged as **local-SNAPSHOT**. You can then run the container using the run script in the root of the project.  If running on a mac you
must use the **-l** flag to switch to local mode as Docker for Mac does not support host network mode (used in production to expose the
spark diagnostic GUI). For example:

```
run_pipeline2_docker.sh -v local-SNAPSHOT -p /your/patient/dir -r /your/reference/file -c /your/config/dir -l
```

### On Crunch
Once you've committed and pushed Travis will build a versioned docker image and push it to dockerhub. These versioned images can be pulled
and run on any server with docker running. The run_pipeline2_docker script is distributed with the scripts repository and on the path of
all crunch servers, so you can run Pipeline2 the same as you run locally. To just use defaults (cancerPanel sample) you only have to run
the following:

```
run_pipeline2_docker.sh -v 0.0.{travis-build-number}
```

