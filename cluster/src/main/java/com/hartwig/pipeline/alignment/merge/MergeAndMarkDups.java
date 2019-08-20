package com.hartwig.pipeline.alignment.merge;

import static com.hartwig.pipeline.alignment.AlignmentOutputPaths.bai;
import static com.hartwig.pipeline.alignment.AlignmentOutputPaths.bam;
import static com.hartwig.pipeline.alignment.AlignmentOutputPaths.sorted;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentOutputPaths;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class MergeAndMarkDups {

    private final ComputeEngine computeEngine;
    private final ResultsDirectory resultsDirectory;
    private final Storage storage;
    private final Arguments arguments;

    public MergeAndMarkDups(final ComputeEngine computeEngine, final ResultsDirectory resultsDirectory, final Storage storage,
            final Arguments arguments) {
        this.computeEngine = computeEngine;
        this.resultsDirectory = resultsDirectory;
        this.storage = storage;
        this.arguments = arguments;
    }

    public AlignmentOutput run(SingleSampleRunMetadata metadata, PerLaneAlignmentOutput alignmentOutput) {
        RuntimeBucket bucket = RuntimeBucket.from(storage, Aligner.NAMESPACE, metadata, arguments);
        List<InputDownload> laneBams = alignmentOutput.bams()
                .stream()
                .flatMap(indexedBamLocation -> Stream.of(new InputDownload(indexedBamLocation.bam()),
                        new InputDownload(indexedBamLocation.bai())))
                .collect(Collectors.toList());
        BashStartupScript bash = BashStartupScript.of(bucket.name());
        laneBams.forEach(bash::addCommand);

        String outputBamPath = AlignmentOutputPaths.sorted(metadata.sampleName());
        GoogleStorageLocation outputBamLocation = GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(outputBamPath));

        bash.addCommand(new SambambaMarkdupsCommand(laneBams.stream().map(InputDownload::getLocalTargetPath).collect(Collectors.toList()),
                outputBamPath)).addCommand(new OutputUpload(outputBamLocation));

        PipelineStatus status = computeEngine.submit(bucket, VirtualMachineJobDefinition.mergeMarkdups(bash, resultsDirectory));

        return AlignmentOutput.builder()
                .status(status)
                .maybeFinalBamLocation(outputBamLocation)
                .maybeFinalBaiLocation(GoogleStorageLocation.of(bucket.name(),
                        resultsDirectory.path(AlignmentOutputPaths.bai(outputBamPath))))
                .addReportComponents(new SingleFileComponent(bucket,
                                Aligner.NAMESPACE,
                                Folder.from(metadata),
                                sorted(metadata.sampleName()),
                                bam(metadata.sampleName()),
                                resultsDirectory),
                        new SingleFileComponent(bucket,
                                Aligner.NAMESPACE,
                                Folder.from(metadata),
                                bai(sorted(metadata.sampleName())),
                                bai(bam(metadata.sampleName())),
                                resultsDirectory))
                .build();
    }
}
