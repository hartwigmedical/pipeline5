package com.hartwig.pipeline.calling.germline;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.execution.vm.*;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.resource.GATKDictAlias;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;

import static java.lang.String.format;

public class GermlineCaller {
    private static final String OUTPUT_FILENAME = "germline_output.gvcf";

    private final Arguments arguments;
    private final ComputeEngine executor;
    private final Storage storage;

    GermlineCaller(final Arguments arguments, final ComputeEngine executor, final Storage storage) {
        this.arguments = arguments;
        this.executor = executor;
        this.storage = storage;
    }

    public GermlineCallerOutput run(AlignmentOutput alignmentOutput) {
        RuntimeBucket bucket = RuntimeBucket.from(storage, alignmentOutput.sample().name(), arguments);
        bucket.cleanup();
        bucket = RuntimeBucket.from(storage, alignmentOutput.sample().name(), arguments);

        Resource referenceGenome = new Resource(storage, arguments.referenceGenomeBucket(), bucket.name(),
                new ReferenceGenomeAlias().andThen(new GATKDictAlias()));
        Resource knownSnps = new Resource(storage, arguments.knownSnpsBucket(), bucket.name());

        BashStartupScript startupScript = BashStartupScript.of(bucket.name())
                .addLine("echo Starting up at $(date)")
                .addCommand(new InputDownload(alignmentOutput.finalBamLocation()))
                .addCommand(new InputDownload(alignmentOutput.finalBaiLocation()))
                .addCommand(new ResourceDownload(knownSnps.copyInto(bucket), bucket))
                .addCommand(new ResourceDownload(referenceGenome.copyInto(bucket), bucket))
                .addCommand(new GatkHaplotypeCaller(format("%s/*.bam", VmDirectories.INPUT),
                        format("%s/*.fasta", VmDirectories.RESOURCES),
                        format("%s/dbsnp_137.b37.vcf", VmDirectories.RESOURCES),
                        format("%s/%s", VmDirectories.OUTPUT, OUTPUT_FILENAME)))
                .addLine("echo Processing finished at $(date)")
                .addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), ResultsDirectory.defaultDirectory().path())));

        executor.submit(bucket, VirtualMachineJobDefinition.germlineCalling(startupScript));
        return GermlineCallerOutput.of(bucket.name(), OUTPUT_FILENAME);
    }
}
