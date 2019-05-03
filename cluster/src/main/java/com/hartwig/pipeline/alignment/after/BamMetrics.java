package com.hartwig.pipeline.alignment.after;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.execution.vm.*;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.NamespacedResults;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.resource.GATKDictAlias;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;

public class BamMetrics {
    private final Arguments arguments;
    private final ComputeEngine executor;
    private final Storage storage;
    private final NamespacedResults namespacedResults;

    BamMetrics(final Arguments arguments, final ComputeEngine executor, final Storage storage, final NamespacedResults results) {
        this.arguments = arguments;
        this.executor = executor;
        this.storage = storage;
        this.namespacedResults = results;
    }

    public BamMetricsOutput run(AlignmentOutput alignmentOutput) {
        RuntimeBucket bucket = RuntimeBucket.from(storage, alignmentOutput.sample().name(), arguments);
        Resource referenceGenome = new Resource(storage,
                arguments.referenceGenomeBucket(),
                bucket.name(),
                new ReferenceGenomeAlias().andThen(new GATKDictAlias()));
        ResourceDownload genomeDownload = new ResourceDownload(referenceGenome.copyInto(bucket), bucket);
        InputDownload bam = new InputDownload(alignmentOutput.finalBamLocation());

        BashStartupScript startup = BashStartupScript.of(bucket.name())
                .addLine("echo Starting up at $(date)")
                .addCommand(new InputDownload(alignmentOutput.finalBamLocation()))
                .addCommand(genomeDownload)
                .addCommand(new BamMetricsCommand(bam.getLocalTargetPath(), genomeDownload.find(".fasta"), alignmentOutput.sample()))
                .addLine("echo Processing finished at $(date)")
                .addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), namespacedResults.path())));

        executor.submit(bucket, VirtualMachineJobDefinition.bamMetrics(startup, namespacedResults));
        return null;
    }
}
