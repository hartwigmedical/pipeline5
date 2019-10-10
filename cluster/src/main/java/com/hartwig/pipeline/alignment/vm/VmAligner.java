package com.hartwig.pipeline.alignment.vm;

import static java.lang.String.format;

import static com.hartwig.pipeline.alignment.AlignmentOutputPaths.bai;
import static com.hartwig.pipeline.alignment.AlignmentOutputPaths.bam;
import static com.hartwig.pipeline.alignment.AlignmentOutputPaths.sorted;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.google.cloud.storage.Storage;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentOutputStorage;
import com.hartwig.pipeline.alignment.ExistingAlignment;
import com.hartwig.pipeline.alignment.sample.SampleData;
import com.hartwig.pipeline.alignment.sample.SampleSource;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.unix.UlimitOpenFilesCommand;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.storage.SampleUpload;
import com.hartwig.pipeline.trace.StageTrace;

public class VmAligner implements Aligner {

    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final SampleSource sampleSource;
    private final SampleUpload sampleUpload;
    private final ResultsDirectory resultsDirectory;
    private final AlignmentOutputStorage alignmentOutputStorage;
    private final ExecutorService executorService;

    public VmAligner(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage, final SampleSource sampleSource,
            final SampleUpload sampleUpload, final ResultsDirectory resultsDirectory, final AlignmentOutputStorage alignmentOutputStorage,
            final ExecutorService executorService) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.sampleSource = sampleSource;
        this.sampleUpload = sampleUpload;
        this.resultsDirectory = resultsDirectory;
        this.alignmentOutputStorage = alignmentOutputStorage;
        this.executorService = executorService;
    }

    @Override
    public AlignmentOutput run(final SingleSampleRunMetadata metadata) throws Exception {
        if (!arguments.runAligner()) {
            return ExistingAlignment.find(metadata, alignmentOutputStorage, arguments);
        }

        StageTrace trace = new StageTrace(NAMESPACE, metadata.sampleName(), StageTrace.ExecutorType.COMPUTE_ENGINE).start();
        RuntimeBucket rootBucket = RuntimeBucket.from(storage, NAMESPACE, metadata, arguments);

        ResourceDownload referenceGenomeDownload =
                ResourceDownload.from(rootBucket, new Resource(storage, arguments.resourceBucket(), ResourceNames.REFERENCE_GENOME));
        String referenceGenomePath = referenceGenomeDownload.find("fa", "fasta");

        SampleData sampleData = sampleSource.sample(metadata);
        Sample sample = sampleData.sample();
        if (arguments.upload()) {
            sampleUpload.run(sample, rootBucket);
        }

        ExecutorService executorService = this.executorService;
        List<Future<PipelineStatus>> futures = new ArrayList<>();
        List<GoogleStorageLocation> perLaneBams = new ArrayList<>();
        for (Lane lane : sampleData.sample().lanes()) {

            RuntimeBucket laneBucket = RuntimeBucket.from(storage, laneNamespace(lane), metadata, arguments);

            BashStartupScript bash = BashStartupScript.of(laneBucket.name());

            InputDownload first =
                    new InputDownload(GoogleStorageLocation.of(rootBucket.name(), fastQFileName(sample.name(), lane.firstOfPairPath())));
            InputDownload second =
                    new InputDownload(GoogleStorageLocation.of(rootBucket.name(), fastQFileName(sample.name(), lane.secondOfPairPath())));

            bash.addCommand(referenceGenomeDownload).addCommand(first).addCommand(second).addCommand(new UlimitOpenFilesCommand(102400));

            SubStageInputOutput alignment = new LaneAlignment(referenceGenomePath,
                    first.getLocalTargetPath(),
                    second.getLocalTargetPath(),
                    sample.name(),
                    lane).apply(SubStageInputOutput.empty(sample.name()));
            perLaneBams.add(GoogleStorageLocation.of(laneBucket.name(), resultsDirectory.path(alignment.outputFile().fileName())));

            bash.addCommands(alignment.bash())
                    .addCommand(new OutputUpload(GoogleStorageLocation.of(laneBucket.name(), resultsDirectory.path())));
            futures.add(executorService.submit(() -> computeEngine.submit(laneBucket,
                    VirtualMachineJobDefinition.alignment(laneId(lane).toLowerCase(), bash, resultsDirectory))));
        }

        AlignmentOutput output;
        if (lanesSuccessfullyComplete(futures)) {

            List<InputDownload> laneBams = perLaneBams.stream().map(InputDownload::new).collect(Collectors.toList());

            BashStartupScript mergeMarkdupsBash = BashStartupScript.of(rootBucket.name());
            laneBams.forEach(mergeMarkdupsBash::addCommand);

            SubStageInputOutput merged = new MergeMarkDups(laneBams.stream()
                    .map(InputDownload::getLocalTargetPath)
                    .filter(path -> path.endsWith("bam"))
                    .collect(Collectors.toList())).apply(SubStageInputOutput.empty(sample.name()));

            mergeMarkdupsBash.addCommands(merged.bash());

            mergeMarkdupsBash.addCommand(new OutputUpload(GoogleStorageLocation.of(rootBucket.name(), resultsDirectory.path())));

            PipelineStatus status =
                    computeEngine.submit(rootBucket, VirtualMachineJobDefinition.mergeMarkdups(mergeMarkdupsBash, resultsDirectory));

            output = AlignmentOutput.builder()
                    .sample(metadata.sampleName())
                    .status(status)
                    .maybeFinalBamLocation(GoogleStorageLocation.of(rootBucket.name(),
                            resultsDirectory.path(merged.outputFile().fileName())))
                    .maybeFinalBaiLocation(GoogleStorageLocation.of(rootBucket.name(),
                            resultsDirectory.path(bai(merged.outputFile().fileName()))))
                    .addReportComponents(new SingleFileComponent(rootBucket,
                                    Aligner.NAMESPACE,
                                    Folder.from(metadata),
                                    sorted(metadata.sampleName()),
                                    bam(metadata.sampleName()),
                                    resultsDirectory),
                            new SingleFileComponent(rootBucket,
                                    Aligner.NAMESPACE,
                                    Folder.from(metadata),
                                    bai(sorted(metadata.sampleName())),
                                    bai(bam(metadata.sampleName())),
                                    resultsDirectory))
                    .build();
        } else {
            output = AlignmentOutput.builder().sample(metadata.sampleName()).status(PipelineStatus.FAILED).build();
        }
        trace.stop();
        executorService.shutdown();
        return output;
    }

    private static String laneNamespace(final Lane lane) {
        return NAMESPACE + "/" + laneId(lane);
    }

    static String laneId(final Lane lane) {
        return lane.flowCellId() + "-" + lane.laneNumber();
    }

    private boolean lanesSuccessfullyComplete(final List<Future<PipelineStatus>> futures) {
        return futures.stream()
                .map(VmAligner::getFuture)
                .filter(status -> status.equals(PipelineStatus.FAILED))
                .collect(Collectors.toList())
                .isEmpty();
    }

    private static String fastQFileName(final String sample, final String fullFastQPath) {
        return format("samples/%s/%s", sample, new File(fullFastQPath).getName());
    }

    private static PipelineStatus getFuture(Future<PipelineStatus> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
