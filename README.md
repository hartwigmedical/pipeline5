# HMF Pipeline version 5
Pipeline 5 (Pv5) is a new data pipeline build to perform secondary and tertiary analysis on patient DNA samples. Pv5 is build on [ADAM](https://adam.readthedocs.io/en/latest/), which is in turn built on [Apache Spark](https://spark.apache.org/). In production Pv5 runs on [Google Cloud Dataproc](https://cloud.google.com/dataproc/), but can also be run as plain old Java or in a standalone docker container.

1. [Building and Testing Pipeline 5](#building-and-testing-pipeline-5)
2. [Pipeline Stages](#pipeline-stages)
3. [Running Locally](#running-pv5-locally)
4. [Running Pv5 on Google Dataproc](#running-pv5-on-google-dataproc)

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

During development or testing it can be useful to run the pipeline in Intellij to debug or get quick feedback on changes. At the time of writing the entire pipeline cannot be run in one command localy, but the individual stages can. Most of the time the BAM creation stage will be run on its own, so this guide focussing on that.

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

The `cluster` module contains all the integration with Google Cloud Dataproc. The main entrypoint to this module is `com.hartwig.pipeline.bootstrap.Bootstrap`. Roughly Bootstrap executes the following:
- Prepares the runtime bucket
- Uploads the input FASTQ if required
- Uploads the executable JAR if required
- Copies static data into the runtime bucket (reference genome and known indel sites)
- Creates a performance profile (ie how big a cluster) based on the input size
- Creates two clusters: single node for gunziping and sorting; one based on the performance profile for BAM creation.
- Gunzips
- Creates BAM
- Composes BAM. This takes advantage of Google Storage's compose feature to quickly combine the BAM shards produced by ADAM.
- Sorts and Indexes BAM
- Optionally downloads the final BAM to local filesystem or SBP Object Store
- Optionally cleans up the runtime bucket

### Runtime Bucket
The runtime bucket is created at startup as a sandbox for all inputs (FASTQ, ref genome, jar) and outputs (BAMs) for the pipeline run. All Google Dataproc temporary files and logs are also written to the runtime bucket.

Each runtime bucket gets a name based on the following convention `gs://run-SAMPLE_ID`. A suffix after `run-` can be added by passing bootstrap the `run_id` flag. This can be useful to annotate experiments or generate a unique id.

Static data is copied into the bucket at startup from some well known buckets available in the project:

| Bucket | Contents|
| ------ | -------- |
| `gs://reference_genome` | The GRCh37 reference genome FASTA and supporting files |
| `gs://known_indels` | Known indel site VCF files |

### Accounts and Permissions
Any GCP project which runs Pv5 requires the following accounts be setup (with these exact names).

| Account | Description | Roles |
| ------- | ----------- | ----- |
| bootstrap | A service account used by the bootstrap process to upload data, manage clusters, submit spark jobs. | Dataproc Editor, Service Account User, Monitoring Admin, Storage Admin, Storage Object Admin |
| dataproc-monitor | A service account used within the running Spark jobs to log metrics to StackDriver | Dataproc Worker, Monitoring Admin |

### Clusters
Two clusters are created in a pipeline run, a single node cluster and one based on the input size. The single node cluster is used for long running stages with limited parallizability. For instance, we only need 16 cores to gunzip 16 files, so no use using a 1000 core Spark cluster. The gunzip and sorting jobs are sent to the cheaper single node cluster, and the BAM creation is handled by the big guy.

### Restart Behaviour
In production we run the bootstrap process in a Kubernetes cluster. Occasionally nodes get restarted and containers killed. To this end the default restart behaviour is to resume best it can from where it left off. This is accomplished using the run-id to check for running clusters and jobs before submitting. Any running cluster
of the same name as the desired cluster will be re-used. If the job is also still running in the cluster, bootstrap will reattach and wait for that job to complete and resume from there. If the job has already run and completed successfully, execution will be skipped. If the job has been cancelled or failed, it will be deleted and
resubmitted.

### Monitoring and Metrics
To troubleshoot issues or determine the progress of a run, there are several relevant log files and metrics you can use.

_Bootstrap Output_
If there are any issues in the interaction between bootstrap and Google, they should be logged in bootstraps stdout/err.

_Google Cloud Console_
The console can be used to see all running Dataproc clusters and jobs. To see the running clusters, use the left navigation menu and find the Big Data section. When a run kicks off you can initially expect to see a cluster with the run_id and suffix `single-node`. When the gunzip is finished the `bam` cluster appears.

The Jobs area of the Dataproc console can be used to see the running spark jobs and their logs. From the list of jobs choose the cluster for which you are interested in (there are never more than one concurrent job per cluster running). A tail of the logs will then be displayed. You can also look at old logs if the `no_cleanup` option has been specified when running bootstrap (as otherwise the runtime bucket is deleted when the process exits).

_Spark UI_
To really get some insight into what the BAM creation job is doing, you can look at the Spark UI. This is a little more involved, and described well [here](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces). The Spark UI will give an overview of exactly what tasks are executing and for how long, Spark's internal plan for execution, memory usage, configuration, etc. [Here](https://mapr.com/blog/getting-started-spark-web-ui/) is a good start tutorial to help you understand what's in there.

_Stackdriver Metrics_
Stackdriver is Google's internal metric and time series monitoring platform. It's a little rough around the edges but does contain all the resource statistics you'd expect in a Cloud platform. That said, it can be hard to see the forest through the trees, as individual node metrics aren't linked to specific clusters or pipeline runs. To this end we also log the following holistic metrics, each tagged with the pipeline's run id.

| Metric | Unit | Description |
| - | - | - |
| COST | USD | Total cost of the pipeline run |
| COST_PER_GB | USD | Cost of the pipeline run divided by the size of the file (normalized)
| FASTQ_SIZE_GB | GB | Total filesize of all input FASTQ (zipped) |
| BOOTSTRAP_SPENT_TIME | MILLIS | Total time spent on the pipeline run |
| BAM_CREATED_SPENT_TIME | MILLIS | Total time spent on just the BAM creation (ie Spark work) |

### SBP Integration
When `sbp_id` is passed to bootstrap the pipeline will take inputs from and push outputs to the SBP object store. This ID corresponds to the sample ID from SBP's REST API. The Object Store is interacted with via `gsutil` and the AWS S3 Java API. It is expected that the correct external configuration for the S3 interface to the Object Store is mounted correctly.

### Running with Docker
Bootstrap is released along with the pipeline jars as a docker container. This way it can be run anywhere with docker installed (or on a Kubernetes cluster as in production). Here is an example useage to run against `COLO829R` on `crunch003`.

```
docker run -v /home/wolfe/:/secrets -v /data2/pipelineinput/COLO829_fastq/COLO829R:/patients/COLO829R/ docker.io/hartwigmedicalfoundation/bootstrap:5.1.358 -no_download
```

The first volume (-v) mounts a directory containing the private key (expected to be called bootstrap-key.json), this is required to authenticate as a service account with Google. You can create a key if you don't have one via the Google console. The second mount is for the FASTQ's to be uploaded, so is not required when running from SBP or against an existing runtime bucket. The `no_download` option is used to disable.

### Options
Bootstrap has a decent amount of command line options you can use. Here is the useage description at the time of writing, but it's advisable to run `bootstrap help` to see the exact options available in the version you are running:

```
usage: bootstrap
 -cloud_sdk <cloud_sdk>                 Path to the google cloud sdk bin
                                        directory (with gsutil and gcloud)
                                        Default is
                                        /usr/lib/google-cloud-sdk/bin
 -cpu_per_gb <cpu_per_gb>               Number of CPUs to use per GB of
                                        FASTQ. Default is 4
 -d <patient_directory>                 Root directory of the patient data
                                        Default is /patients/
 -force_jar_upload                      Force upload of JAR even if the
                                        version already exists in cloud
                                        storage
 -k <private_key_path>                  Fully qualified path to the
                                        private key for the service
                                        account usedfor all Google Cloud
                                        operations Default is
                                        /secrets/bootstrap-key.json
 -l <jar_lib_directory>                 Directory containing the
                                        system-{VERSION}.jar. Default is
                                        /usr/share/pipeline5/
 -no_cleanup                            Don't delete the cluster or
                                        runtime bucket after job is
                                        complete
 -no_download                           Do not download the final BAM from
                                        Google Storage. Will also leave
                                        the runtime bucket in place
 -no_upload                             Don't upload the sample to
                                        storage. This should be used in
                                        combination with a run_id which
                                        points at an existing bucket
 -node_init_script <node_init_script>   Script to run on initialization of
                                        each cluster node. The default
                                        script installs BWA Default is
                                        node-init.sh
 -p <patient>                           ID of the patient to process.
                                        Default is empty
 -project <project>                     The Google project for which to
                                        create the cluster. Default is
                                        hmf-pipeline-development
 -reference_genome <reference_genome>   Bucket from which to copy the
                                        reference genome into the runtime
                                        bucket. Just a name, not a url (no
                                        gs://) Default is reference_genome
 -region <region>                       The region in which to create the
                                        cluster. Default is europe-west4
 -run_id <run_id>                       Override the generated run id used
                                        for runtime bucket and cluster
                                        naming
 -s3_upload_threads <s3_upload_threads> Number of threads to use in
                                        parallelizing uploading of large
                                        files (multipart) to S3 Default
                                        is 10
 -sbp_api_url <sbp_api_url>             URL of the SBP API endpoint
                                        Default is http://hmfapi
 -sbp_s3_url <sbp_s3_url>               URL of the SBP S3 endpoint Default
                                        is http://hmfapi
 -sbp_sample_id <sbp_sample_id>         SBP API internal numeric sample id
 -skip_upload                           Skip uploading of patient data
                                        into cloud storeage
 -use_preemtible_vms                    Allocate half the cluster as
                                        preemtible VMs to save cost. These
                                        VMs can be reclaimed at any time
                                        so can be unstable
 -v <version>                           Version of pipeline5 to run in
                                        spark. Default is empty
 -verbose_cloud_sdk                     Have stdout and stderr from Google
                                        tools like gsutil strem to the
                                        console
```