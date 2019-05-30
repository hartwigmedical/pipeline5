package com.hartwig.pipeline.snpgenotype;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.*;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.resource.GATKDictAlias;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceNames;

import static java.lang.String.format;

public class SnpGenotype {

    public static final String NAMESPACE = "snp_genotype";

    private static final String OUTPUT_FILENAME = "snp_genotype_output.vcf";

    private final Arguments arguments;
    private final ComputeEngine executor;
    private final Storage storage;
    private final ResultsDirectory resultsDirectory;

    public SnpGenotype(final Arguments arguments, final ComputeEngine executor, final Storage storage,
                final ResultsDirectory resultsDirectory) {
        this.arguments = arguments;
        this.executor = executor;
        this.storage = storage;
        this.resultsDirectory = resultsDirectory;
    }

    public SnpGenotypeOutput run(AlignmentOutput alignmentOutput) {

        // argument to disable this step
        if (!arguments.runSnpGenotyper()) {
            return SnpGenotypeOutput.builder().status(JobStatus.SKIPPED).build();
        }

        // grab sample name
        String sampleName = alignmentOutput.sample().name();

        // create a bucket for resources, and outputs of this step
        RuntimeBucket bucket = RuntimeBucket.from(storage, NAMESPACE, sampleName, arguments);

        // Get "resources" you need look at gs://common-resources/strelka_config/
        Resource referenceGenome = new Resource(storage,
                arguments.resourceBucket(),
                ResourceNames.REFERENCE_GENOME,
                new ReferenceGenomeAlias().andThen(new GATKDictAlias()));
        Resource genotypeSnps = new Resource(storage, arguments.resourceBucket(), ResourceNames.GENOTYPE_SNPS);

        BashStartupScript startupScript = BashStartupScript.of(bucket.name())
                .addLine("echo Starting up at $(date)")

                // download inputs "bams"
                .addCommand(new InputDownload(alignmentOutput.finalBamLocation()))
                .addCommand(new InputDownload(alignmentOutput.finalBaiLocation()))

                // download resources to VM
                .addCommand(new ResourceDownload(genotypeSnps.copyInto(bucket)))
                .addCommand(new ResourceDownload(referenceGenome.copyInto(bucket)))

                // Run GATK
                .addCommand(new SnpGenotypeCommand(format("%s/*.bam", VmDirectories.INPUT),
                        format("%s/*.fasta", VmDirectories.RESOURCES),
                        format("%s/26SNPtaq.vcf", VmDirectories.RESOURCES),
                        format("%s/%s", VmDirectories.OUTPUT, OUTPUT_FILENAME)))
                .addLine("echo Processing finished at $(date)")

                // upload output back to GS
                .addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path())));

        ImmutableSnpGenotypeOutput.Builder outputBuilder = SnpGenotypeOutput.builder();

        // pass the bash to a real VM
        JobStatus status = executor.submit(bucket, VirtualMachineJobDefinition.snpGenptyping(startupScript, resultsDirectory));

        // return locations of output
        return outputBuilder.status(status)
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, sampleName, resultsDirectory))
                .addReportComponents(new SingleFileComponent(bucket, NAMESPACE, sampleName, OUTPUT_FILENAME, resultsDirectory))
                .build();
    }
}
