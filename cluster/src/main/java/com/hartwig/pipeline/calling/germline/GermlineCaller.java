package com.hartwig.pipeline.calling.germline;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.*;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.resource.GATKDictAlias;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;

import static java.lang.String.format;

public class GermlineCaller {

    private static final String NAMESPACE = "germline_caller";

    private static final String OUTPUT_FILENAME = "germline_output.gvcf";

    private final Arguments arguments;
    private final ComputeEngine executor;
    private final Storage storage;
    private final ResultsDirectory resultsDirectory;

    GermlineCaller(final Arguments arguments, final ComputeEngine executor, final Storage storage,
            final ResultsDirectory resultsDirectory) {
        this.arguments = arguments;
        this.executor = executor;
        this.storage = storage;
        this.resultsDirectory = resultsDirectory;
    }

    public GermlineCallerOutput run(AlignmentOutput alignmentOutput) {
        RuntimeBucket bucket = RuntimeBucket.from(storage, NAMESPACE, alignmentOutput.sample().name(), arguments);

        Resource referenceGenome = new Resource(storage,
                arguments.referenceGenomeBucket(), arguments.referenceGenomeBucket(), new ReferenceGenomeAlias().andThen(new GATKDictAlias()));
        Resource knownSnps = new Resource(storage, arguments.knownSnpsBucket(),arguments.knownSnpsBucket());

        BashStartupScript startupScript = BashStartupScript.of(bucket.name())
                .addLine("echo Starting up at $(date)")
                .addCommand(new InputDownload(alignmentOutput.finalBamLocation()))
                .addCommand(new InputDownload(alignmentOutput.finalBaiLocation()))
                .addCommand(new ResourceDownload(knownSnps.copyInto(bucket)))
                .addCommand(new ResourceDownload(referenceGenome.copyInto(bucket)))
                .addCommand(new GatkHaplotypeCallerCommand(format("%s/*.bam", VmDirectories.INPUT),
                        format("%s/*.fasta", VmDirectories.RESOURCES),
                        format("%s/dbsnp_137.b37.vcf", VmDirectories.RESOURCES),
                        format("%s/%s", VmDirectories.OUTPUT, OUTPUT_FILENAME)))
                .addLine("echo Processing finished at $(date)")
                .addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path())));

        ImmutableGermlineCallerOutput.Builder outputBuilder = GermlineCallerOutput.builder();
        JobStatus status = executor.submit(bucket, VirtualMachineJobDefinition.germlineCalling(startupScript, resultsDirectory));
        if (status.equals(JobStatus.SUCCESS)) {
            outputBuilder.germlineVcf(GoogleStorageLocation.of(bucket.name(), OUTPUT_FILENAME));
        }
        return outputBuilder.status(status).build();
    }
}
