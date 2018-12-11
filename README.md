# HMF Pipeline version 5
Pipeline 5 (Pv5) is a new data pipeline build to perform secondary and tertiary analysis on patient DNA samples. Pv5 is build on [ADAM](https://adam.readthedocs.io/en/latest/), which is in turn built on [Apache Spark](https://spark.apache.org/). In production Pv5 runs on [Google Cloud Dataproc](https://cloud.google.com/dataproc/), but can also be run as plain old Java or in a standalone docker container.

## Building and Testing Pipeline 5

The only pre-requisite for running the build is having bwa installed locally (for the ADAM functional tests). For Mac users bwa can be
installed easily via homebrew, and there are also packages for most linux distributions. All else fails you can build from source
http://bio-bwa.sourceforge.net/

Pipeline 5 is built with maven. To run all tests and also build the Docker image run the following:

```
mvn clean install
```
Building the Docker image is quite time consuming and probably not necessary for most testing (see next section). To disable the building
of the Docker image run the following.
```
mvn clean install -DskipDocker
```

## Pipeline Stages

Pv5 is split into 3 stages at the time of writing.
- Gunzipping of Files
- BAM creation
- Sorting and Indexing

This split is for practical reasons. For instance gunzipping 16 FASTQ files has quite a different performance profile to read alignment with BWA.  The split is meant to use enable the more efficient use of resources.

### Gunzip
Uses Spark to read the zipped files and then write them back to HDFS in the default partition structure. This not only unzips the files, but allows BAM creation to effectively parallelize tasks across the data.

### Bam Creation
Uses ADAM to transform FASTQ files into BAM files with BWA. BWA is run against all partitions of the FASTQ data in parallel which speeds up this step considerably. When complete the ADAM mark duplicate and realign indels algoritms are also run.

### Sorting and Indexing
At the time of writing we've been unable to get ADAM's native sorting to perform reliably and as fast as [sambamba](http://lomereiter.github.io/sambamba/). While we address this with ADAM developers, this stage sorts and indexes the BAM with sambamba as a post-processing stage.

## Running Pv5 locally

During development or testing it can be useful to run the pipeline in Intellij to debug or get quick feedback on changes. At the time of writing the entire pipeline cannot be run in one command locallty, but the individual stages can. Most of the time the BAM creation stage will be run on its own, so this guide focussing on that.

### Configuration

Pipeline 5 expects a yaml file `conf/pipeline.yaml` relative to the processes working directory. See `system/src/test/resources/configuration/all_parameters/conf/pipeline.yaml`
for an example of this file. Within the yaml file you can configure the following:

| Parameter               | Default | Description
| ----------------------- | ------- | -------------------------------------
| **pipeline**            | | Parameters which impact the running of the pipeline
| hdfs                 | `file:///` | HDFS url location in which the input is located and output will be stored
| saveResultsAsSingleFile | `false` | Merged the sharded BAM using ADAM. Defaulted to false as in Dataproc, we use Google's compose feature to merge the result much faster than ADAM (which is single threaded)
| results                 | `/results` | Relative directory to the runtime in which the results are stored
| **spark**               | | Parameters specific to Apache Spark
| master | `local[1]` |  The spark master user (ie local[#cpus], yarn, spark url, etc)
| key:value | N/A | Arbitrary key value pairs corresponding to [Spark configuration](https://spark.apache.org/docs/latest/configuration.html) options.
| **patient**             | | Parameters to configure the patient data. Note, this is currently a misnomer as BAM creation actually works on a sample basis not a patient. To be refactored soon...
| name                    | `none` | Name of the patient/sample
| directory               | `/patients` | Directory of patient FASTQ files.
| **referenceGenome** || Location of the reference genome
| directory               | `reference_genome` | Directory containing reference genome FASTA file
| file                    | `none` ||Name of reference genome FASTA file
| **knownIndel** || Location of known indel sites for indel realignment
| directory               | `/known_indels` | Directory containing known indel vcf files
| files                   | `empty list` | A YAML list of known indel vcf files.

The simplest way to run the pipeline locally is to simply run PipelineRuntime main class from IntelliJ. The working directory will be the
root of the project so just add your /conf directory there as well.

## Running Pv5 on Google Dataproc

The `cluster` module contains all the integration with Google Cloud Dataproc.
