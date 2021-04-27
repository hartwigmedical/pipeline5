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

#### SnpGenotype (GATK UnifiedGenotyper)
Also used in final QC, GATK's
[UnifiedGenotyper](https://software.broadinstitute.org/gatk/documentation/tooldocs/3.8-0/org_broadinstitute_gatk_tools_walkers_genotyper_UnifiedGenotyper.php)
is used to call variants around 26 specific locations. The results are used as a final sanity check of the pipeline output.

#### Germline Calling (GATK HaplotypeCaller)
GATK's
[HaplotypeCaller](https://software.broadinstitute.org/gatk/documentation/tooldocs/3.8-0/org_broadinstitute_gatk_tools_walkers_haplotypecaller_HaplotypeCaller.php)
is used to call germline variants on the reference sample only.

#### Amber
[Amber](https://github.com/hartwigmedical/hmftools/tree/master/amber) is an HMF in-house tool used mainly to determine 
the minor allele copy number of heterozygous germline variants in the tumor sample.

#### Cobalt
[Cobalt](https://github.com/hartwigmedical/hmftools/tree/master/cobalt) is an HMF in-house tool used to determine read
depth ratios.

#### Somatic Variant Calling (SAGE)
[SAGE](https://github.com/hartwigmedical/hmftools/tree/master/sage) is an HMF in-house tool used to call 
somatic variants (SNVs and Indels) between the tumor/reference pair.

#### Structural Variant Calling (GRIDSS)
[GRIDSS](https://github.com/PapenfussLab/gridss) is used to call structural variants between the tumor/reference pair.

#### Structural Variant Filtering (GRIPSS)
[GRIPSS](https://github.com/hartwigmedical/hmftools/tree/master/gripss) is used to extract only the somatic variants from the full structural variant call set 
and remove all low quality calls.

#### Purple
[Purple](https://github.com/hartwigmedical/hmftools/tree/master/purple) is an HMF in-house tool which combines
the read-depth ratios, BAF, and variants to produce the pipeline final output, used both in the final report and exposed to
research.

#### LINX
[LINX](https://github.com/hartwigmedical/hmftools/tree/master/sv-linx) is an HMF in-house tool which interprets structural variants 
and calls fusions and homozygously disrupted genes.

#### CHORD
[CHORD](https://github.com/UMCUGenetics/CHORD) is an external tool which evaluates whether the tumor is HR-deficient based on the complete set of variants found.

#### Health Check
[Health Checker](https://github.com/hartwigmedical/hmftools/tree/master/health-checker) is an HMF in-house doing a final QC based off the purple results and BAM metrics.

## Developers Guide

### Building and Testing Pipeline 5

Pv5 is a Java application and is built with Maven. It is compatible with all builds of [Java 11](https://jdk.java.net/11/) and [Maven
3](https://maven.apache.org/download.cgi) (if in doubt just use the latest).

To build the application and run all the tests:

```
mvn clean install -DskipDocker
```
Note the `-DskipDocker`. Building the Docker image is time consuming and not necessary for most testing. Without this flag a
Docker image will be built for you based on the version of the artifact in your root `pom.xml`.

### CI with Travis

After each push our CI build runs on [Travis](https://travis-ci.org/). The build is basically the equivalent of running `mvn
clean install`. On each CI build we:
- Compile all the modules.
- Run all the tests
- Create a docker image which can be used to run the code in production.

Each docker image gets a version of 5.{minor}.{build-number}. 

### GIT and Pull Requests

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