# Hartwig Medical Foundation - Pipeline5

Pipeline5 (Pv5) is a processing and analysis pipeline for high throughput DNA sequencing data used in Hartwig Medical Foundation's (HMF) 
patient processing and research. The goals of the project are to deliver best in class performance and scalability using the Google Cloud Platform

### Overview of Pipeline Stages
The pipeline first runs primary and secondary analysis on a reference (blood/normal) sample and tumor sample before comparing
them in the final somatic pipeline. Steps 1.4.1-1.4.5 are run in the single sample pipeline, where 1.4.6-1.4.15 are run in the
somatic. 

#### Alignment
Under normal circumstances Pv5 starts with the input of one to _n_ paired-end FASTQ files produced by sequencing. The first task
of the pipeline is to align these reads to the human reference genome (using the BWA algorithm). We use the largest core counts
available and parallelise by sample (tumor/normal) and lane within each sample to achieve reasonable performance. 

Mark duplicates and sorting is performed after the per lane alignments with [Sambamba](https://lomereiter.github.io/sambamba/).

#### WGS Metrics
Our downstream QC tools require certain metrics about the BAM. These are produced using Picard tools
[CollectWgsMetrics](https://software.broadinstitute.org/gatk/documentation/tooldocs/4.0.0.0/picard_analysis_CollectWgsMetrics.php).

#### Samtools Flagstat
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

#### 1.4.6 Amber
[Amber](https://github.com/hartwigmedical/hmftools/tree/master/amber) is an HMF in-house tool used mainly to determine 
the minor allele copy number of heterzygous germline variants in the tumor sample.

#### 1.4.7 Cobalt
[Cobalt](https://github.com/hartwigmedical/hmftools/tree/master/count-bam-lines) is an HMF in-house tool used to determine read
depth ratios.

#### 1.4.8 Somatic Variant Calling (SAGE)
[SAGE](https://github.com/hartwigmedical/hmftools/tree/master/sage) is an HMF in-house tool used to call 
somatic variants (SNVs and Indels) between the tumor/reference pair.

#### 1.4.9 Structural Variant Calling (GRIDSS)
[GRIDSS](https://github.com/PapenfussLab/gridss) is used to call structural variants between the tumor/reference pair.

#### 1.4.10 Structural Variant Filtering (GRIPSS)
[GRIPSS](https://github.com/hartwigmedical/hmftools/tree/master/gripss) is used to extract only the somatic variants from the full structural variant call set 
and remove all low quality calls.

#### 1.4.11 Purple
[Purple](https://github.com/hartwigmedical/hmftools/tree/master/purity-ploidy-estimator) is an HMF in-house tool which combines
the read-depth ratios, BAF, and variants to produce the pipeline final output, used both in the final report and exposed to
research.

#### 1.4.12 LINX
[LINX](https://github.com/hartwigmedical/hmftools/tree/master/sv-linx) is an HMF in-house tool which interprets structural variants 
and calls fusions and homozygously disrupted genes.

#### 1.4.13 Bachelor
[Bachelor](https://github.com/hartwigmedical/hmftools/tree/master/bachelor) is an HMF in-house tool which looks for pathogenic germline variants 
and annotates them with their state in the tumor sample.

#### 1.4.14 CHORD
[CHORD](https://github.com/UMCUGenetics/CHORD) is an external tool which evaluates whether the tumor is HR-deficient based on the complete set of variants found.

#### 1.4.15 Health Check
[Health Checker](https://github.com/hartwigmedical/hmftools/tree/master/health-checker) is an HMF in-house doing a final QC based off the purple results and BAM metrics.

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