# HMF Pipeline2
Phase 1 of the pipeline redesign will involve evaluating different technological approaches using a minimum viable product (MVP). This document outlines the MVP and criteria we’ll use to evaluate the merits of a particular approach.

This MVP is focussed on the functional aspects of the project by creating a working prototype that produces useful output. That said, a criteria for success is portability, so the solution needs to be executable modern compute infrastructures. Evaluation of different IaaS options is not in scope of this initial MVP, but will follow soon after.

## Functional Scope
Functionally the MVP will perform a subset of the pipeline focussed on producing an analyzable BAM file:
* Read a set of 8 input FASTQ file pairs.
* For each paired input map with BWA to single output BAM file.
* For each mapped input sort by chromosome and position producing an output BAM file.
* Combine 8 BAM files and mark duplicates producing a merged BAM file.
* Perform local re-alignment of reads around indels, producing a realigned BAM file.

The BAM file produced by the process can then already be tested against the remaining analysis pipeline steps.

## Technologies Under Evaluation
The following technologies will be evaluated for use in the MVP. This document does not prescribe the exact combination and number of MVP iterations, but gives some rationale as to why the particular tool is chosen for evaluation.

### Apache Spark (https://spark.apache.org/)
Spark will be evaluated as the underlying distributed computing engine. Spark has become the defacto technology for parallel processing of big datasets by providing a rich functional DSL and a fast, reliable runtime. It is easy to run locally and to automate testing. It is currently the most active Apache project, which bodes well for continued innovation and support.

Spark can be run in-memory or in a cluster. Clusters can be run on Hadoop, Mesos, Kubernetes or Spark’s own standalone cluster. Spark is interoperable with just about every storage technology we might want to use. This gives us a lot of flexibility for the next steps.

Both GATK4 and ADAM run on top of Spark. Spark can also pipe native processes outside its JVM which could be used to run the pipeline in a similar fashion to the current implementation. This should let us combine GATK, ADAM, out-of-process and our own tools within the same Spark pipeline.

### GATK4 (https://software.broadinstitute.org/gatk/)
GATK4 is the latest genome toolkit version now running with Spark as its execution engine. We currently use GATK3 for base recalibration, indel realignment, and germline calling/filtering. GATK3 is no longer actively developed so this is a good opportunity to evaluate the new version for our pipeline. GATK4 should also be faster and more reliable. GATK is widely adopted and well maintained by the Broad Institute.

The GATK platform provides its own complete pipeline framework and cloud runtime. It is not likely that we’ll use the entire framework, but tools and parts of the architecture as libraries.

### ADAM (https://github.com/bigdatagenomics/adam)
ADAM is another genomics toolkit which also uses Spark as its underlying engine. While the GATK is maintained singularly by the Broad Institute, ADAM is more of a consolidation of projects to achieve a more holistic goal. An interesting differentiator between GATK is that ADAM can store its data in a combination of Avro and Parquet. Parquet provides a columnar storage format with good compression. Avro is a serialization mechanism which is self-describing, easing interoperability and data evolution. By using these standards over proprietary formats, ADAM allows for easier integration with the full suite of Spark and big data processing tools.

Should be noted that ADAM is still in a 0.x version and less widely used than GATK.

### Docker (https://www.docker.com/)
Docker is a containerization platform we will evaluate to deploy the released software. By creating containers we can encapsulate the entire runtime into an immutable binary, reducing variance in deployment and local execution. Docker integrates with a wide array of cloud computing platforms.

The simplest alternative to Docker is a jar or tar or multiple jars with a single entry point. Docker will only be evaluated if we deem it necessary based on the prior choices, as it does add an extra layer of complexity.

## Evaluation Criteria
The MVP will be evaluated on the following:

| Requirement             | Criteria
| ----------------------- | ---------------------------------------------
| Functional Completeness | Pipeline2 BAM output compared with Pipeline1
| Performance Improvement | Pipeline2 benchmarked against Pipeline1
| Testability             | Pipeline2 can be tested end to end during build process.
| Deployability           | Pipeline2 can be installed/upgraded in a single step.
| Scalability             | Pipeline2 can be scaled horizontally across samples
| Portability             | Pipeline2 can be run on leading IaaS providers (in particular AWS EMR and Google Dataproc)

At this point monetary cost is not yet evaluated, that will also come in the next phase when we looks into the IaaS side of things.

## Methodology

The MVP codebase can be found in a new public HMF repository here: https://github.com/hartwigmedical/pipeline2. It is a Java codebase using HMF coding standards and build tooling.

The first step will be to create a small framework to execute a Spark pipeline with pluggable stages. We’ll take a test driven approach to measure correctness and performance of the pipeline permutations.

For more details of the progress and evolution of the project please keep an eye on the repository and reach out with questions or suggestions whenever they come up. I’ll be monitoring #ext-hartwig-pipeline on Slack for this purpose. My hope is to spend one month on this evaluation process, but consider that a rough estimate at this point.