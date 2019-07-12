# Hartwig Medical Foundation - Pipeline5

1. [Technical Overview](#technical-overview)
2. [Developers Guide](#developers-guide)

## 1. Technical Overview

### 1.1 Introduction
Pipeline5 (Pv5) is a processing and analysis pipeline for high throughput DNA sequencing data used in Hartwig Medical Foundation's (HMF) patient processing and research. The goals of the project are to deliver best in class performance and scalability using modern big data techniques and cloud infrastructure. To this end, Pv5 uses ADAM, Spark and Google Cloud Platform. Following is an overview of how they come together to form the Pv5 architecture.

### 1.2 Google Cloud Platform
We chose Google Cloud Platform as our cloud infrastructure provider for the following reasons (evaluated fall 2018):
- GCP had the best and simplest pricing model for our workload.
- GCP had the most user friendly console for monitoring and operations.
- GCP had the fastest startup time for its managed Hadoop service.

The last point was important to us due to a design choice made to address resource contention. Pv5 uses ephemeral clusters tailored to each patient. This way at any point we are only using exactly the resources we need for our workload, never having to queue or idle. To make this possible we spin up large clusters quickly as new patients come in and tear them down again once the patient processing is complete.

Pv5 makes use of the following GCP services:
- [Google Cloud Storage](https://cloud.google.com/storage/) to store transient patient data, supporting resources, configuration and tools.
- [Google Dataproc](https://cloud.google.com/dataproc/) to run ADAM/Spark workloads, currently only used in alignment (more on this later).
- [Google Compute Engine](https://cloud.google.com/compute/) to run workloads using tools not yet compatible with Spark (samtools, GATK, strelka, etc)

### 1.3 ADAM, Spark and Dataproc
[ADAM](https://github.com/bigdatagenomics/adam) is a genomic analysis modeling and processing framework built on Apache Spark. Please see [the documentation](https://adam.readthedocs.io/en/latest/) for a complete description of the ADAM ecosystem and goals. At this point we only use the core APIs and datamodels. [Apache Spark](https://spark.apache.org/) is an analytics engine used for large scale data processing, also best to read their own docs for a complete description.

We use ADAM to parallelize processing of FASTQ and BAM files using hadoop. ADAM provides an avro datamodel to read and persist this data from and to HDFS, and the ability to hold the massive datasets in memory as they are processed.

Google Cloud Platform offers [Dataproc](https://cloud.google.com/dataproc/) as a managed spark environment. Each patient gets its own Dataproc cluster which is started when their FASTQ lands and deleted when tertiary analysis is complete. Dataproc includes a connector to use Google Storage as HDFS. With this we can have these transient compute clusters, with permanent distributed data storage.

### 1.4 Google Compute Engine
Not all the algorithms in our pipeline are currently suited to ADAM. For these tools we’ve developed a small framework to run them on VMs in Java. To accomplish this we’ve created a standard VM image containing a complete repository of external and internal tools, and OS dependencies.

Using internal APIs we launch VM jobs by generating a bash startup script which will copy inputs and resources, run the tools themselves, and copy the final results (or any errors) back up into google storage.

VMs performance profiles can be created to use Google’s standard machine type or custom cpu/ram combinations based on the workload’s requirements.

### 1.5 Pipeline Stages
The pipeline first runs primary and secondary analysis on a reference (blood/normal) sample and tumor sample before comparing them in the final somatic pipeline. Steps 1.5.1-1.5.5 are run in the single sample pipeline, where 1.5.6-1.5.11 are run in the somatic. Alignment is the only step which uses Google Dataproc, while the rest run in vanilla Google Compute Engine.

#### 1.5.1 Alignment
Under normal circumstances Pv5 starts with the input of one to n paired-end FASTQ files produced by sequencing. The first task of the pipeline is to align these reads to the human reference genome. The algorithm behind this process is BWA. This is where we most take advantage of ADAM, Spark and Dataproc. ADAM gives us an API to run external tools against small blocks of data in parallel. Using this API we run thousands of BWA processes in parallel then combine the alignment results into a single BAM file and persist it.

Worth noting there is both a pre-processing and post-processing step done here. Before alignment, we run a small Spark cluster to simply gunzip the data. Our input FASTQs come in gzipp’ed (not bgzipped) so cannot be properly parallelized by Spark in that state. After the alignment is complete, we run a sambamba sort and index, as the ADAM sort we’ve found unstable and does not perform indexing.
#### 1.5.2 WGS Metrics
Our downstream QC tools require certain metrics about the BAM. These are produced using Picard tools [CollectWgsMetrics](https://software.broadinstitute.org/gatk/documentation/tooldocs/4.0.0.0/picard_analysis_CollectWgsMetrics.php).
#### 1.5.3 Samtools Flagstat
[Samtools](http://www.htslib.org/doc/samtools.html) flag statistics are not consumed by any downstream stages, but very useful in ad hoc QC and analysis.
#### 1.5.4 SnpGenotype (GATK UnifiedGenotyper)
Also used in final QC, GATK’s [UnifiedGenotyper](https://software.broadinstitute.org/gatk/documentation/tooldocs/3.8-0/org_broadinstitute_gatk_tools_walkers_genotyper_UnifiedGenotyper.php) is used to call variants around 26 specific location. The results are used as a final sanity check of the pipeline output.
#### 1.5.5 Germline Calling (GATK HaplotypeCaller)
GATk’s [HaplotypeCaller](https://software.broadinstitute.org/gatk/documentation/tooldocs/3.8-0/org_broadinstitute_gatk_tools_walkers_haplotypecaller_HaplotypeCaller.php) is used to call germline variants on the reference sample only.
#### 1.5.6 Somatic Variant Calling (Strelka)
[Strelka](https://github.com/Illumina/strelka) from illumina is used to call SNP and INDEL variants between the tumor/reference pair.
#### 1.5.7 Structural Variant Calling (GRIDSS)
[GRIDSS](https://github.com/PapenfussLab/gridss) is used to call structural variants between the tumor/reference pair.
#### 1.5.8 Cobalt
[Cobalt](https://github.com/hartwigmedical/hmftools/tree/master/count-bam-lines) is an HMF in-house tool used to determine read depth ratios.
#### 1.5.8 Amber
[Amber](https://github.com/hartwigmedical/hmftools/tree/master/amber) is an HMF in-house tool used to determine the B allele frequency of a tumor/reference pair.
#### 1.5.9 Purple
[Purple](https://github.com/hartwigmedical/hmftools/tree/master/purity-ploidy-estimator) is an HMF in-house tool which combines the read-depth ratios, BAF, and variants to produce the pipeline final output, used both in the final report and exposed to research.
#### 1.5.10 Health Check
Final QC of the purple results and BAM metrics.

### 1.7 Production Environment at Schuberg Phillis

[Schuberg Philis](https://schubergphilis.com/) manages our pipeline operations. The production deployment of Pv5 is run in their environment. SBP provides orchestration tooling to manage the ingestion of FASTQ data from the HMF labs and metadata management of the samples, runs and output files. They also administer the HMF GCP projects.

## 2. Developers Guide

### 2.1 Building and Testing Pipeline 5

Pv5 is a Java application and is built with Maven. It is compatible with all builds of [Java 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and [Maven 3](https://maven.apache.org/download.cgi) (if in doubt just use the latest).

Otherwise only pre-requisite for running the build is having bwa installed locally (for the ADAM functional tests). For Mac users bwa can be installed easily via homebrew, and there are also packages for most linux distributions. All else fails you can build from source
http://bio-bwa.sourceforge.net/

To build the application and run all thes tests do the following

```
mvn clean install -DskipDocker
```
Note the `skipDocker`. Building the Docker image is time consuming and probably not necessary for most testing. If you remove this flag a docker image will be built for you based on the version of the artifact in your root pom.xml

### 2.2 CI with Travis

After each push our CI build runs on [Travis](https://travis-ci.org/). The build is basically the equivalent of running `mvn clean install`. On each CI build we:
- Compile all the modules.
- Run all the tests
- Create a shaded jar for all code to be run on Spark.
- Create a docker image which can be used to run the code in production.

Each docker image gets a version of 5.{minor}.{build-number}. Every build should produce a releasable image, although not every release ends up in project (ie. we're not doing continuous delivery at this point). We do not use branches for different releases, only a single master.

### 2.3 GIT and Pull Requests

We use PRs to implement a pre-commit review process, not for long-lived feature branches. When working on a ticket for which you wish to have reviewed before ending up in production:
- Create a ticket for you work in [JIRA](https://hartwigmedical.atlassian.net/secure/Dashboard.jspa)
- Create a branch matching the name of the ticket.
- Do some cool stuff
- Push your new branch, make sure to expose it remotely
- GitHub should detect your push to a branch and show you a button to create a PR at the top of the repo.
- Assign a reviewer to your new PR.
- Address any comments, then merge

Small changes do not need to follow this process, its meant to help and not hinder productivity. Use your best judgement.

All changes should be committed with quality commit messages, as this is a main source of documentation. Please the [7 rules](https://chris.beams.io/posts/git-commit/) and follow them.