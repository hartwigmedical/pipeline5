# Hartwig Medical Foundation - Pipeline5

1. [Technical Overview](#1-technical-overview)
2. [Developers Guide](#2-developers-guide)
3. [Operators Guide](#3-operators-guide)

## 1 Technical Overview

### 1.1 Introduction
Pipeline5 (Pv5) is a processing and analysis pipeline for high throughput DNA sequencing data used in Hartwig Medical
Foundation's (HMF) patient processing and research. The goals of the project are to deliver best in class performance and
scalability using modern big data techniques and cloud infrastructure. To this end, Pv5 uses ADAM, Spark and Google Cloud
Platform. Following is an overview of how they come together to form the Pv5 architecture.

### 1.2 Google Cloud Platform
We chose Google Cloud Platform as our cloud infrastructure provider for the following reasons (evaluated fall 2018):
- GCP had the best and simplest pricing model for our workload.
- GCP had the most user friendly console for monitoring and operations.
- GCP had the fastest startup time for its managed Hadoop service.

The last point was important to us due to a design choice made to address resource contention. Pv5 uses ephemeral clusters
tailored to each patient. This way at any point we are only using exactly the resources we need for our workload, never having
to queue or idle. To make this possible we spin up large clusters quickly as new patients come in and tear them down again once
the patient processing is complete.

Pv5 makes use of the following GCP services:
- [Google Cloud Storage](https://cloud.google.com/storage/) to store transient patient data, supporting resources, configuration and tools.
- [Google Dataproc](https://cloud.google.com/dataproc/) to run ADAM/Spark workloads, currently only used in alignment (more on this later).
- [Google Compute Engine](https://cloud.google.com/compute/) to run workloads using tools not yet compatible with Spark
  (samtools, GATK, strelka, etc)

### 1.3 ADAM, Spark and Dataproc
[ADAM](https://github.com/bigdatagenomics/adam) is a genomic analysis modeling and processing framework built on Apache Spark.
Please see [the documentation](https://adam.readthedocs.io/en/latest/) for a complete description of the ADAM ecosystem and
goals. At this point we only use the core APIs and datamodels. [Apache Spark](https://spark.apache.org/) is an analytics engine
used for large scale data processing, also best to read their own docs for a complete description.

We use ADAM to parallelize processing of FASTQ and BAM files using hadoop. ADAM provides an avro datamodel to read and persist
this data from and to HDFS, and the ability to hold the massive datasets in memory as they are processed.

Google Cloud Platform offers [Dataproc](https://cloud.google.com/dataproc/) as a managed spark environment. Each patient gets
its own Dataproc cluster which is started when their FASTQ lands and deleted when tertiary analysis is complete. Dataproc
includes a connector to use Google Storage as HDFS. With this we can have these transient compute clusters, with permanent
distributed data storage.

### 1.4 Google Compute Engine
Not all the algorithms in our pipeline are currently suited to ADAM. For these tools we've developed a small framework to run
them on VMs in Java. To accomplish this we've created a standard VM image containing a complete repository of external and
internal tools, and OS dependencies.

Using internal APIs we launch VM jobs by generating a bash startup script which will copy inputs and resources, run the tools
themselves, and copy the final results (or any errors) back up into google storage.

VMs performance profiles can be created to use Google's standard machine type or custom cpu/ram combinations based on the
workload's requirements.

### 1.5 Pipeline Stages
The pipeline first runs primary and secondary analysis on a reference (blood/normal) sample and tumor sample before comparing
them in the final somatic pipeline. Steps 1.5.1-1.5.5 are run in the single sample pipeline, where 1.5.6-1.5.11 are run in the
somatic. Alignment is the only step which uses Google Dataproc, while the others run in Google Compute Engine on virtual
machines.

#### 1.5.1 Alignment
Under normal circumstances Pv5 starts with the input of one to _n_ paired-end FASTQ files produced by sequencing. The first task
of the pipeline is to align these reads to the human reference genome (using the BWA algorithm). This is where we most take
advantage of ADAM, Spark and Dataproc. ADAM gives us an API to run external tools against small blocks of data in parallel.
Using this API we run thousands of BWA processes in parallel then combine the alignment results into a single BAM file and
persist it.

It is worth noting there are both a pre-processing and post-processing step done here. Before alignment, we run a small Spark
cluster to simply gunzip the data. Our input FASTQs come in gzipp-ed (not bgzipped) so cannot be properly parallelized by Spark
in that state. After the alignment is complete, we run a sambamba sort and index, as the ADAM sort we've found unstable and does
not perform indexing.  

#### 1.5.2 WGS Metrics
Our downstream QC tools require certain metrics about the BAM. These are produced using Picard tools
[CollectWgsMetrics](https://software.broadinstitute.org/gatk/documentation/tooldocs/4.0.0.0/picard_analysis_CollectWgsMetrics.php).

#### 1.5.3 Samtools Flagstat
[Samtools](http://www.htslib.org/doc/samtools.html) flag statistics are not consumed by any downstream stages, but very useful
in ad hoc QC and analysis.  

#### 1.5.4 SnpGenotype (GATK UnifiedGenotyper)
Also used in final QC, GATK's
[UnifiedGenotyper](https://software.broadinstitute.org/gatk/documentation/tooldocs/3.8-0/org_broadinstitute_gatk_tools_walkers_genotyper_UnifiedGenotyper.php)
is used to call variants around 26 specific locations. The results are used as a final sanity check of the pipeline output.

#### 1.5.5 Germline Calling (GATK HaplotypeCaller)
GATK's
[HaplotypeCaller](https://software.broadinstitute.org/gatk/documentation/tooldocs/3.8-0/org_broadinstitute_gatk_tools_walkers_haplotypecaller_HaplotypeCaller.php)
is used to call germline variants on the reference sample only.

#### 1.5.6 Somatic Variant Calling (Strelka)
[Strelka](https://github.com/Illumina/strelka) from illumina is used to call SNP and INDEL variants between the tumor/reference pair.

#### 1.5.7 Structural Variant Calling (GRIDSS)
[GRIDSS](https://github.com/PapenfussLab/gridss) is used to call structural variants between the tumor/reference pair.

#### 1.5.8 Cobalt
[Cobalt](https://github.com/hartwigmedical/hmftools/tree/master/count-bam-lines) is an HMF in-house tool used to determine read
depth ratios.

#### 1.5.8 Amber
[Amber](https://github.com/hartwigmedical/hmftools/tree/master/amber) is an HMF in-house tool used to determine the B allele
frequency of a tumor/reference pair.

#### 1.5.9 Purple
[Purple](https://github.com/hartwigmedical/hmftools/tree/master/purity-ploidy-estimator) is an HMF in-house tool which combines
the read-depth ratios, BAF, and variants to produce the pipeline final output, used both in the final report and exposed to
research.

#### 1.5.10 Health Check
Final QC of the purple results and BAM metrics.

### 1.7 Production Environment at Schuberg Phillis

[Schuberg Philis](https://schubergphilis.com/) manages our pipeline operations. The production deployment of Pv5 is run in their
environment. SBP provides orchestration tooling to manage the ingestion of FASTQ data from the HMF labs and metadata management
of the samples, runs and output files. They also administer the HMF GCP projects.

### 1.8 Data and Compute Topology

The following diagram gives a simple overview of the dataflow between components, their executions platforms, and locations (ie what runs
in GCP vs SBP private cloud). Dotted arrows represent API calls (create, start, stop, delete, etc) and solid dataflow. Three key pieces
of data are shown here:
- FASTQ - Raw sequencer output.
- BAM - Aligned sequence output produced by alignment stage.
- VCF - Variants called by the pipeline and other final output files

![alt text](https://github.com/hartwigmedical/pipeline5/blob/master/pv5_architecture_overview.png "Pv5 Topology")

## 2 Developers Guide

### 2.1 Building and Testing Pipeline 5

Pv5 is a Java application and is built with Maven. It is compatible with all builds of [Java
8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and [Maven
3](https://maven.apache.org/download.cgi) (if in doubt just use the latest).

Otherwise only pre-requisite for running the build is having bwa installed locally (for the ADAM functional tests). For Mac
users bwa can be installed easily via homebrew, and there are also packages for most linux distributions. All else fails you can
build from source http://bio-bwa.sourceforge.net/

To build the application and run all thes tests do the following

```
mvn clean install -DskipDocker
```
Note the `-DskipDocker`. Building the Docker image is time consuming and not necessary for most testing. Without this flag a
Docker image will be built for you based on the version of the artifact in your root `pom.xml`.

### 2.2 CI with Travis

After each push our CI build runs on [Travis](https://travis-ci.org/). The build is basically the equivalent of running `mvn
clean install`. On each CI build we:
- Compile all the modules.
- Run all the tests
- Create a shaded jar for all code to be run on Spark.
- Create a docker image which can be used to run the code in production.

Each docker image gets a version of 5.{minor}.{build-number}. Every build should produce a releasable image, although not every
release ends up in project (ie. we're not doing continuous delivery at this point). We do not use branches for different
releases, only a single master.

### 2.3 GIT and Pull Requests

We use PRs to implement a pre-commit review process, not for long-lived feature branches. When working on a ticket for which you
wish to have reviewed before ending up in production:

- Create a ticket for your work in [JIRA](https://hartwigmedical.atlassian.net/secure/Dashboard.jspa)
- Create a branch matching the name of the ticket.
- Do some cool stuff
- Push your new branch, making sure to expose it remotely
- GitHub should detect your push to a branch and show you a button to create a PR at the top of the repo.
- Assign a reviewer to your new PR.
- Address any comments, then merge

Small changes do not need to follow this process, its meant to help and not hinder productivity. Use your best judgement.

All changes should be committed with quality commit messages, as this is a main source of documentation. Please read the [7
rules](https://chris.beams.io/posts/git-commit/) and follow them.

## 3 Operators Guide

### 3.1 Overview

This section documents the moving parts in the production environment. At the highest level the Pv5 processing flow
consists of three steps:

1. FASTQ file(s) become available and are copied to Google Cloud Storage (GCS). Pv5 is run (in Google Cloud Platform (GCP))
   against them in single-sample mode.
1. When a single-sample run for at least one normal and one tumor are complete, Pv5 is invoked in somatic mode against the output
   data from the single-sample runs.
1. Resulting data from the runs are collected into a final output structure, submitted back to SBP's storage cloud and 
   registered with SBP's API.

#### 3.1.1 Operational Terminology

These terms will be used for the rest of the operational guide:

- A *sample* is the all data from a physical tissue sample for a patient
- A *set* is all of the data available for a particular patient, the metadata that is needed to associate the constituent samples
    to one another, and the execution of the pipeline for that set
- A *run* is an invocation of the pipeline against a particular sample of a set, in a given mode
- A *stage* is a part of a run which executes on either its own DataProc cluster or virtual machine in GCP
- The *project* in GCP is the mechanism we use to keep development separate from production. While all the developers and probably
    some operators have access to the development project, the production project is limited to operators.

#### 3.1.2 Service Accounts

GCP access can be managed in multiple ways but we are using [service
accounts](https://cloud.google.com/compute/docs/access/service-accounts) to avoid maintenance and security overheads.
Operationally a JSON file containing the private key needs to be downloaded from the GCP console; that file can either be placed
in default location so it does not need to be specified on the command line, or that file can be mentioned by name on the command 
line. See [Invocation](#3.5-invocation).

The current production service account is called `pipeline5-scheduler`.

### 3.2 Storage Buckets

GCS makes a bucket seem more like a filesystem (or parent directory), with folders (or directories) and files, than some of the
other cloud storage products out there. Here we'll treat buckets as cloud-backed filesystems.

There are two sorts of buckets that are created when a run happens. Runtime buckets contain in-flight data that is required during
the run's duration, while output buckets contain the finished results for one or many runs.

Two other buckets are also involved in Pv5 runs: a "tools" bucket which stores the software that is used to generate a disk image
for our VMs, and a "resources" bucket which holds data required by the runs.

#### 3.2.1 Runtime Buckets

As the runs of a set are executing, the pipeline creates buckets to hold the input and output of the stages of the runs. From an 
operational perspective the pipeline may be thought of as a series of operations, with the outputs from earlier stages becoming
the inputs for later ones. Each stage in a run will get a diretory which will contain:

- Input data, which comes from previous stages' outputs or as input to the pipeline;
- Results, which will be used as input to later stages or as final output of the pipeline;
- Resource data, such as the reference genome or configuration files;
- Administrative and control files (completion flags, log files, etc).

The pipeline creates and manages buckets as required to support the operations that are selected for a given run. In a typical
complete run comprising single-sample normal and tumor stages and the somatic stage, there will be three runtime buckets. The
buckets are easy to find as they are named according to the sample and set data provided on the command line to the application, for
example:

```
run-cpct12345678r
run-cpct12345678t
run-cpct12345678r-cpct12345678t
```

#### 3.2.2 Output Buckets

When each stage of a run completes successfully the pipeline copies any results which are part of the finished pipeline to a
folder in the output bucket for the set to which the run belongs. The output bucket may be controlled via arguments to the 
pipeline application.

In addition to the results from each stage, the output bucket will also contain in its top-level directory the log from the
coordinating pipeline process, the set metadata, and some version information for the pipeline itself in order to make it easy to
reproduce runs in the future and for auditing purposes.

The output bucket can be configured at runtime with the `-patient_report_bucket`. The default for production is `pipeline-output-prod`.

#### 3.2.3 Tools Bucket

There is a tools bucket for each environment (eg development, production). This bucket is used at VM disk image creation time (see
below) and contains all software that is not part of the base OS but which is required by any stage of the pipeline that is run
under a VM. Deployment to production will typically involve promoting the contents of the development tools bucket to the
production tools bucket.

The tools bucket can be configured at runtime with `-tools_bucket`. The default for production is `common-tools-prod`.

#### 3.2.4 Resources Bucket

This contains data which is needed by the runs but which is not specific to a particular run. Examples would be white- and
black-lists for genome regions, the reference genome currently in use, etc.

While the tools bucket is used only at image-creation time, this bucket is used by multiple stages in every run. However as with
the tools bucket this bucket's contents must be promoted from development to production when a deployment is done.

The resources bucket can be configured at runtime with `-resource_bucket`. The default for production is `common-resources-prod`.

### 3.3 Virtual Machines

Pv5 uses custom VMs to run all stages of the pipeline that are not run under Google's
[Dataproc](https://cloud.google.com/dataproc/) offering. Most stages require more flexibility than we can easily get from running
a cluster and shelling out from Spark and thus are a better fit for running on virtual machines.

#### 3.3.1 Custom Disk Images

Our VMs are run against a customised version of one of the Google base virtual machine disk images. We start with their image as
it provides something that is configured to run properly under their infrastructure then "freeze" off our customisations into our
own disk image. This image will need to be updated from time to time using our image generation script, which automates these
steps:

1. A new VM is started using the latest version of the preferred Google disk image family.
1. The script executes the set of commands in the custom commands list maintained in the source repository. The list has commands
   to install OS packages and software from the tools bucket as well as anything else that may be needed.
1. The VM is stopped and a custom image taken from its disk contents. This custom image is stored in Google's infrastructure under
   the project.

When the coordinator requests a new VM be created for a stage of the pipeline, the latest member of the series of custom images
will be used. Therefore a new image must be cut whenever any tools change in development, and as a matter of procedure any
production deployment should include a run of the imaging script against the production project.

The image family is named `disk-imager-standard`. All versions can be seen in the GCP Console, Compute Engine->Images.

#### 3.3.2 Disk Layout

We have isolated the "working" area of the disk images to the `/data/` directory. There are four directories under this top
level:

- `input` stores the files required by the stage, which will be copied into this location from the runtime bucket when the VM
    instance starts
- `resources` stores resources required by the stage from the resources bucket, which also will be copied from the runtime bucket
    when the VM starts
- `tools` is populated at image creation time with select contents from the tools bucket
- `output` will contain the results of the run, which will be copied up to the output bucket when the stage completes

Beyond these directories only the `/tmp` directory is expected to be used for anything as some of the stages have it set as their
temporary or working directory.

### 3.4 Promotion of a New Version

As very little is shared between projects in GCP any changes to the Pv5 code or dependencies requires a promotion of the relevant
parts of the development project to the production project. This means:

1. Backing up the current production `tools` bucket and promoting the current contents of the development bucket to production;
1. Repeating for the `resources` bucket;
1. Running the image creation script against the production project.

See the `backup_to_bucket.sh`, `create_custom_image.sh` and `promote_environment.sh` scripts in `cluster/images`

### 3.5 Invocation

Our build produces a Docker container that can be used to run the pipeline in production. 

Pv5 runs are controlled via command line arguments to the main program which can be used to override the defaults. A subset of
the arguments are defaulted to empty because a useful default does not exist; a value will need to be explicitly specified for
the run for these. As of this writing this subset contains:

- The `-set_id`, which is needed to tie the run back to metadata in the SBP API when the run completes and we want to upload the
    results.
- The `-sample_id`, needed to indicate which sample belonging to the set is being processed by this part of the run.
- For the somatic phase, the program needs to be told to run in the somatic mode (see the [technical
    overview](#1-technical-overview) section above). Pass `-mode somatic` on the command line.

Other arguments may be desirable or this list may have changed by the time you read this; run the application with the `-help`
argument to see a full list.

### 3.6 Troubleshooting and Bug Reports

Having discussed the output and runtime buckets and invocation options, it is now possible to provide some general troubleshooting
and escalation procedures. If something goes wrong with a run:

- The pipeline log in the top level of the output bucket will contain information on the versions of the various components that
    are in use, the success/failure status of each of the stages in the run, and any exceptions that may have occurred on the
    coordinator instance itself.
- Failure messages should be captured in the logs for each stage, which will be valuable for root-cause analysis for any failed
    stage.
- The output bucket will contain metadata and versioning information.

Before logging any support requests all of the above should be inspected for useful information. If the cause of a failure cannot
be determined, the same should be forwarded with any bug report.

