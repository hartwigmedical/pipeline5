package com.hartwig.pipeline.bammetrics;

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

public class BamMetrics {

    static final String NAMESPACE = "bam_metrics";
    private final Arguments arguments;
    private final ComputeEngine executor;
    private final Storage storage;
    private final ResultsDirectory resultsDirectory;

    BamMetrics(final Arguments arguments, final ComputeEngine executor, final Storage storage, final ResultsDirectory results) {
        this.arguments = arguments;
        this.executor = executor;
        this.storage = storage;
        this.resultsDirectory = results;
    }

    public BamMetricsOutput run(AlignmentOutput alignmentOutput) {

        if (!arguments.runBamMetrics()) {
            return BamMetricsOutput.builder().status(JobStatus.SKIPPED).build();
        }

        RuntimeBucket bucket = RuntimeBucket.from(storage, NAMESPACE, alignmentOutput.sample().name(), arguments);
        Resource referenceGenome = new Resource(storage,
                arguments.referenceGenomeBucket(),
                arguments.referenceGenomeBucket(),
                new ReferenceGenomeAlias().andThen(new GATKDictAlias()));
        ResourceDownload genomeDownload = new ResourceDownload(referenceGenome.copyInto(bucket));
        InputDownload bam = new InputDownload(alignmentOutput.finalBamLocation());

        String outputFile = BamMetricsOutput.outputFile(alignmentOutput.sample());
        BashStartupScript startup = BashStartupScript.of(bucket.name())
                .addLine("echo Starting up at $(date)")
                .addCommand(new InputDownload(alignmentOutput.finalBamLocation()))
                .addCommand(genomeDownload)
                .addCommand(new BamMetricsCommand(bam.getLocalTargetPath(),
                        genomeDownload.find(".fasta"),
                        VmDirectories.OUTPUT + "/" + outputFile))
                .addLine("echo Processing finished at $(date)")
                .addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path())));

        JobStatus status = executor.submit(bucket, VirtualMachineJobDefinition.bamMetrics(startup, resultsDirectory));
        return BamMetricsOutput.builder()
                .status(status)
                .maybeMetricsOutputFile(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(outputFile)))
                .build();
    }
}
