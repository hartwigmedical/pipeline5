# Hartwig Medical Foundation - Pipeline5

1. [Technical Overview](#1-technical-overview)
2. [Developers Guide](#2-developers-guide)
3. [Operators Guide](#3-operators-guide)

---
**NOTE**

As of version 5.4 we have stopped using ADAM, Spark and DataProc in favour of also running BWA on our VM management framework. 

We were able to achieve similar performance with this approach, at a fraction of the cost, using pre-emptible VMs and local SSDs.

We still love ADAM and hope to find a use for it in the future!

---

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
- [Google Compute Engine](https://cloud.google.com/compute/) to run workloads.

### 1.3 Google Compute Engine
We've developed a small framework in Java to run the pipeline components on virtual machines (VMs). We use a custom disk image
containing a complete repository of external and internal tools and resources, and OS dependencies.

Using internal APIs we launch VM jobs by generating a `bash` startup script which will copy inputs, run the tools themselves, and
copy the final results (or any errors) back up into Google storage.

VM performance profiles can be created to use Google's standard machine type or custom CPU/RAM combinations based on the
workload's requirements.

### 1.4 Pipeline Stages
The pipeline first runs primary and secondary analysis on a reference (blood/normal) sample and tumor sample before comparing
them in the final somatic pipeline. Steps 1.4.1-1.4.5 are run in the single sample pipeline, where 1.4.6-1.4.11 are run in the
somatic. 

#### 1.4.1 Alignment
Under normal circumstances Pv5 starts with the input of one to _n_ paired-end FASTQ files produced by sequencing. The first task
of the pipeline is to align these reads to the human reference genome (using the BWA algorithm). We use the largest core counts
available and parallelise by sample (tumor/normal) and lane within each sample to achieve reasonable performance.

It is worth noting there are both a pre-processing and post-processing step done here. After the alignment is complete, we run a
sambamba sort and index.

#### 1.4.2 WGS Metrics
Our downstream QC tools require certain metrics about the BAM. These are produced using Picard tools
[CollectWgsMetrics](https://software.broadinstitute.org/gatk/documentation/tooldocs/4.0.0.0/picard_analysis_CollectWgsMetrics.php).

#### 1.4.3 Samtools Flagstat
[Samtools](http://www.htslib.org/doc/samtools.html) flag statistics are not consumed by any downstream stages, but useful
in ad hoc QC and analysis.  

#### 1.4.4 SnpGenotype (GATK UnifiedGenotyper)
Also used in final QC, GATK's
[UnifiedGenotyper](https://software.broadinstitute.org/gatk/documentation/tooldocs/3.8-0/org_broadinstitute_gatk_tools_walkers_genotyper_UnifiedGenotyper.php)
is used to call variants around 26 specific locations. The results are used as a final sanity check of the pipeline output.

#### 1.4.5 Germline Calling (GATK HaplotypeCaller)
GATK's
[HaplotypeCaller](https://software.broadinstitute.org/gatk/documentation/tooldocs/3.8-0/org_broadinstitute_gatk_tools_walkers_haplotypecaller_HaplotypeCaller.php)
is used to call germline variants on the reference sample only.

#### 1.4.6 Somatic Variant Calling (SAGE)
[SAGE](https://github.com/hartwigmedical/hmftools/tree/master/sage) is an HMF in-house tool used to call 
somatic variants (SNVs and Indels) between the tumor/reference pair.

#### 1.4.7 Structural Variant Calling (GRIDSS)
[GRIDSS](https://github.com/PapenfussLab/gridss) is used to call structural variants between the tumor/reference pair.

#### 1.4.8 Cobalt
[Cobalt](https://github.com/hartwigmedical/hmftools/tree/master/count-bam-lines) is an HMF in-house tool used to determine read
depth ratios.

#### 1.4.8 Amber
[Amber](https://github.com/hartwigmedical/hmftools/tree/master/amber) is an HMF in-house tool used to determine the B allele
frequency of a tumor/reference pair.

#### 1.4.9 Purple
[Purple](https://github.com/hartwigmedical/hmftools/tree/master/purity-ploidy-estimator) is an HMF in-house tool which combines
the read-depth ratios, BAF, and variants to produce the pipeline final output, used both in the final report and exposed to
research.

#### 1.4.10 Health Check
Final QC of the purple results and BAM metrics.

### 1.5 Data and Compute Topology

The following diagram gives a simple overview of the dataflow between components, their executions platforms, and locations (ie what runs
in GCP vs SBP private cloud). Dotted arrows represent API calls (create, start, stop, delete, etc) and solid dataflow. Three key pieces
of data are shown here:
- FASTQ - Raw sequencer output.
- BAM - Aligned sequence output produced by alignment stage.
- VCF - Variants called by the pipeline and other final output files

![alt text](https://github.com/hartwigmedical/pipeline5/blob/master/pv5_architecture_overview.png "Pv5 Topology")

## 2 Developers Guide

### 2.1 Building and Testing Pipeline 5

Pv5 is a Java application and is built with Maven. It is compatible with all builds of [Java 11](https://jdk.java.net/11/) and [Maven
3](https://maven.apache.org/download.cgi) (if in doubt just use the latest).

To build the application and run all the tests:

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

Small changes do not need to follow this process, it is meant to help and not hinder productivity. Use your best judgement.

All changes should be committed with quality commit messages, as this is a main source of documentation. Please read the [7
rules](https://chris.beams.io/posts/git-commit/) and follow them.

### 2.4 Development Checklist

There is some non-linearity in the development process because if you're adding new features to the image, you have to build an
image _before_ you push your code to the repository if you want to test it. As mentioned the pipeline code will use the latest
version of the image, but this is a little non-obvious when you're developing. We hope to improve the coupling between the code
and the image in future.

A good example scenario is adding a new version of an existing tool. This is an overview of what you would need to do as a
developer:

1. Add the new version of the tool to the tools bucket in the development environment. Since we're adding a new version of an
   existing tool it should be obvious where to put your new file. The bucket is referenced in the image creation script below if
   you're unsure of where to find it. IMPORTANT: add to the bucket but do not delete or move anything; reasoning discussed below.
1. Have a look at the `pipeline5.cmds` file and the `create_custom_image.sh` script, both in the `cluster/images` subdirectory of
   the code repository. Update the commands file if needed to perform additional actions when creating the image.
1. Make the code changes in a branch that should correspond to a JIRA ticket.
1. Run the `create_custom_image.sh` script to create a new image. Note that this is tied to no branch, the image just gets created
   with the current release and a timestamp.
1. Submit your code changes for review to a new remote branch following the guidelines above on `git` and pull requests.

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
line. See [Invocation](#3.5.3-invocation).

The current production service account is called `pipeline5-scheduler`.

### 3.2 Storage Buckets

GCS makes a bucket seem more like a filesystem (or parent directory), with folders (or directories) and files, than some of the
other cloud storage products out there. Here we'll treat buckets as cloud-backed filesystems.

There are two sorts of buckets that are created when a run happens. Runtime buckets contain in-flight data that is required during
the run's duration, while output buckets contain the finished results for one or many runs.

Two other buckets are also involved in Pv5 development, a "tools" bucket which stores the software that is used to generate a disk image
for our VMs, and a "resources" bucket which holds data required by the runs. These are discussed further down.

#### 3.2.1 Runtime Buckets

As the runs of a set are executing, the pipeline creates buckets to hold the input and output of the stages of the runs. From an 
operational perspective the pipeline may be thought of as a series of operations, with the outputs from earlier stages becoming
the inputs for later ones. Each stage in a run will get a directory which will contain:

- Input data, which comes from previous stages' outputs or as input to the pipeline;
- Results, which will be used as input to later stages or as final output of the pipeline;
- Administrative and control files (completion flags, log files, etc).

The pipeline creates and manages buckets as required to support the operations that are selected for a given run. In a typical
complete run comprising single-sample normal and tumor stages and the somatic stage, there will be three runtime buckets. The
buckets are easy to find as they are named according to the sample barcode (if available) and set data provided on the command line to the
application, for example:

```
run-fr11111111
run-fr22222222
run-fr11111111-fr22222222
```

#### 3.2.2 Output Buckets

When each stage of a run completes successfully the pipeline copies any results which are part of the finished pipeline to a
folder in the output bucket for the set to which the run belongs. The output bucket may be controlled via arguments to the 
pipeline application.

In addition to the results from each stage, the output bucket will also contain in its top-level directory the log from the
coordinating pipeline process, the set metadata, and some version information for the pipeline itself in order to make it easy to
reproduce runs in the future and for auditing purposes.

The output bucket can be configured at runtime with the `-patient_report_bucket`. The default for production is `pipeline-output-prod`.

### 3.3 Custom Disk Images

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
will be used. Therefore a new image must be cut whenever any tools change in development. Release notes should include the version
of the image that is to be imported to production for a release.

The image family name starts with `pipeline5`. All versions can be seen in the GCP Console, Compute Engine->Images.

#### 3.3.1 Disk Layout

We have isolated the "working" area of the disk images to the `/data/` directory. There are four directories under this top
level:

- `input` stores the files required by the stage, which will be copied into this location from the runtime bucket when the VM
    instance starts
- `output` will contain the results of the run, which will be copied up to the output bucket when the stage completes

The tools and resources are copied into `/opt/tools` and `/opt/resources` respectively.

Beyond these directories only the `/tmp` directory is expected to be used for anything as some of the stages have it set as their
temporary or working directory.

### 3.4 Promotion of a New Version

Updating the in-use version of the pipeline has two components:

1. Updating the version of the Docker container that encapsulates the controlling process;
1. Ensuring the disk image referenced corresponds to the new version of the Docker container.

On the first point, the CI environment takes care of creating a Docker container that encapsulates the executable portion of the
pipeline. See below for more on Docker.

For the second item, disk images are created in development according to need. See the discussion in the development guide above.
When a release is being made both the code itself and the image must be promoted to the production environment. The required
disk image version will be in the release notes. 

There are scripts for this purpose in the `cluster/images` directory. First run `export_gcp_image.sh` to package the image into a
file in a bucket. Then run `import_gcp_image.sh` (making sure to switch the the destination GCP project using the appropriate
`gcloud auth` commands first!) to import the image into the production project.

### 3.5 Running Pv5

#### 3.5.1 Docker Container

Our build produces a Docker container that can be used to run the pipeline in production. You will need to be familiar with basic
Docker usage before attempting to run the pipeline.

#### 3.5.2 Command-line Arguments

Pv5 runs are controlled via command line arguments to the main program which can be used to override the defaults. A subset of
the arguments are defaulted to empty because a useful default does not exist; a value will need to be explicitly specified for
the run for these. As of this writing this subset contains:

- The `-set_id`, which is needed to tie the run back to metadata in the SBP API when the run completes and we want to upload the
    results.
- The `-sample_id`, needed to indicate which sample belonging to the set is being processed by this part of the run.

Other arguments may be desirable or the list may have changed by the time you read this; run the application with the `-help`
argument to see a full list.

#### 3.5.3 Invocation

The pipeline requires some files to be accessible to run, namely the private key JSON file from GCP and the FASTQ sample files for
the run. You will need to use one of the available mechanisms to make these files available to the container (`-v` to share a
host-side directory with the container, a custom Dockerfile, etc) and possibly some overrides to the command line when you invoke
the application.

An invocation might look like this:

```
$ docker run -v /my/key/dir:/secrets hartwigmedicalfoundation/pipeline5:5.1.123 -set_id myset -sample_id CPCT12345678
```

where:

- The previously-downloaded private key file from GCP has been placed in the local `/my/key/dir` directory (and the pipeline
    will just use the default path).
- The version of the pipeline to run is `5.1.123`. If a specific version is not specified, the latest tagged version will be used.
- Everything after the image name is an argument to the pipeline application itself. 

Command line arguments for Pv5 may be discovered like this:

```
$ docker run hartwigmedicalfoundation/pipeline5:5.1.123 -help
```

### 3.6 Troubleshooting and Bug Reports

Having discussed the output and runtime buckets and invocation options, it is now possible to provide some general troubleshooting
and escalation procedures. If something goes wrong with a run:

- The pipeline log in the top level of the output bucket will contain information on the versions of the various components that
    are in use, the success/failure status of each of the stages in the run, and any exceptions that may have occurred on the
    coordinator instance itself.
- Failure messages should be captured in the logs for each stage, which will be valuable for root-cause analysis for any failed
    stage.
- The output bucket will contain metadata and versioning information.

Before logging any support requests all of the above should be inspected for useful information. The same should be forwarded with 
any bug report.

