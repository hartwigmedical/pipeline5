package com.hartwig.pipeline.alignment.bwa;

import static java.lang.String.format;

import static com.hartwig.pipeline.datatypes.FileTypes.bai;
import static com.hartwig.pipeline.datatypes.FileTypes.bam;
import static com.hartwig.pipeline.resource.ResourceFilesFactory.buildResourceFiles;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.google.cloud.storage.Storage;
import com.hartwig.computeengine.execution.ComputeEngineStatus;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.ComputeEngine;
import com.hartwig.computeengine.execution.vm.RuntimeFiles;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.execution.vm.command.OutputUploadCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.computeengine.storage.RuntimeBucketOptions;
import com.hartwig.pdl.LaneInput;
import com.hartwig.pdl.PipelineInput;
import com.hartwig.pdl.SampleInput;
import com.hartwig.pipeline.ArgumentUtil;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.ImmutableAlignmentOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
import com.hartwig.pipeline.failsafe.DefaultBackoffPolicy;
import com.hartwig.pipeline.input.Inputs;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.labels.Labels;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.OutputComponent;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.output.SingleFileComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.SampleUpload;
import com.hartwig.pipeline.trace.StageTrace;

import net.jodah.failsafe.Failsafe;

public class BwaAligner implements Aligner {

    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final PipelineInput input;
    private final SampleUpload sampleUpload;
    private final ResultsDirectory resultsDirectory;
    private final ExecutorService executorService;
    private final Labels labels;

    public BwaAligner(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage,
            final PipelineInput input, final SampleUpload sampleUpload, final ResultsDirectory resultsDirectory,
            final ExecutorService executorService, final Labels labels) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.input = input;
        this.sampleUpload = sampleUpload;
        this.resultsDirectory = resultsDirectory;
        this.executorService = executorService;
        this.labels = labels;
    }

    public AlignmentOutput run(final SingleSampleRunMetadata metadata) throws Exception {

        StageTrace trace =
                new StageTrace(NAMESPACE, metadata.sampleName(), StageTrace.ExecutorType.COMPUTE_ENGINE).start();
        RuntimeBucket rootBucket = createRuntimeBucket(NAMESPACE, metadata);
        SampleInput sample = Inputs.sampleFor(input, metadata);
        if (sample.bam().isPresent()) {
            return AlignmentOutput.builder()
                    .sample(metadata.sampleName())
                    .status(PipelineStatus.PROVIDED)
                    .maybeAlignments(GoogleStorageLocation.from(sample.bam().get(), arguments.project()))
                    .build();
        }
        final ResourceFiles resourceFiles = buildResourceFiles(arguments);
        sampleUpload.run(sample, rootBucket);

        List<Future<ComputeEngineStatus>> futures = new ArrayList<>();
        List<GoogleStorageLocation> perLaneBams = new ArrayList<>();
        List<OutputComponent> laneLogComponents = new ArrayList<>();
        List<GoogleStorageLocation> laneFailedLogs = new ArrayList<>();
        for (LaneInput lane : sample.lanes()) {

            RuntimeBucket laneBucket = createRuntimeBucket(laneNamespace(lane), metadata);

            BashStartupScript bash = BashStartupScript.of(laneBucket.name());

            InputDownloadCommand first = new InputDownloadCommand(GoogleStorageLocation.of(rootBucket.name(),
                    fastQFileName(sample.name(), lane.firstOfPairPath())));
            InputDownloadCommand second = new InputDownloadCommand(GoogleStorageLocation.of(rootBucket.name(),
                    fastQFileName(sample.name(), lane.secondOfPairPath())));
            bash.addCommand(first).addCommand(second);

            SubStageInputOutput alignment = new LaneAlignment(arguments.sbpApiRunId().isPresent(),
                    resourceFiles.refGenomeFile(),
                    first.getLocalTargetPath(),
                    second.getLocalTargetPath(),
                    lane).apply(SubStageInputOutput.empty(metadata.sampleName()));
            perLaneBams.add(GoogleStorageLocation.of(laneBucket.name(),
                    resultsDirectory.path(alignment.outputFile().fileName())));

            bash.addCommands(alignment.bash())
                    .addCommand(new OutputUploadCommand(GoogleStorageLocation.of(laneBucket.name(), resultsDirectory.path()),
                            RuntimeFiles.typical()));
            var pipelineFuture = executorService.submit(() -> runWithRetries(metadata,
                    laneBucket,
                    VirtualMachineJobDefinitions.alignment(bash, resultsDirectory, "aligner-" + laneId(lane).toLowerCase())));
            futures.add(pipelineFuture);
            laneLogComponents.add(new RunLogComponent(laneBucket,
                    laneNamespace(lane),
                    Folder.from(metadata),
                    resultsDirectory));
            laneFailedLogs.add(GoogleStorageLocation.of(laneBucket.name(), RunLogComponent.LOG_FILE));
        }

        AlignmentOutput output;
        if (lanesSuccessfullyComplete(futures)) {

            List<InputDownloadCommand> laneBams = perLaneBams.stream().map(InputDownloadCommand::new).collect(Collectors.toList());

            List<InputDownloadCommand> laneBamIndexs = perLaneBams.stream()
                    .map(x -> x.transform(FileTypes::bai))
                    .map(InputDownloadCommand::new).collect(Collectors.toList());

            BashStartupScript mergeMarkdupsBash = BashStartupScript.of(rootBucket.name());
            laneBams.forEach(mergeMarkdupsBash::addCommand);
            laneBamIndexs.forEach(mergeMarkdupsBash::addCommand);

            List<String> laneBamPaths = laneBams.stream()
                    .map(InputDownloadCommand::getLocalTargetPath)
                    .filter(path -> path.endsWith("bam"))
                    .collect(Collectors.toList());

            SubStageInputOutput merged = new MergeMarkDups(metadata.sampleName(), resourceFiles, laneBamPaths)
                    .apply(SubStageInputOutput.empty(metadata.sampleName()));

            mergeMarkdupsBash.addCommands(merged.bash());

            mergeMarkdupsBash.addCommand(new OutputUploadCommand(GoogleStorageLocation.of(rootBucket.name(),
                    resultsDirectory.path()), RuntimeFiles.typical()));

            ComputeEngineStatus computeEngineStatus =
                    runWithRetries(metadata, rootBucket, VirtualMachineJobDefinitions.mergeMarkdups(mergeMarkdupsBash, resultsDirectory));

            PipelineStatus pipelineStatus = PipelineStatus.of(computeEngineStatus);

            ImmutableAlignmentOutput.Builder outputBuilder = AlignmentOutput.builder()
                    .sample(metadata.sampleName())
                    .status(pipelineStatus)
                    .maybeAlignments(GoogleStorageLocation.of(rootBucket.name(), resultsDirectory.path(merged.outputFile().fileName())))
                    .addAllReportComponents(laneLogComponents)
                    .addAllFailedLogLocations(laneFailedLogs)
                    .addFailedLogLocations(GoogleStorageLocation.of(rootBucket.name(), RunLogComponent.LOG_FILE))
                    .addReportComponents(new RunLogComponent(rootBucket, Aligner.NAMESPACE, Folder.from(metadata), resultsDirectory));
            if (!arguments.outputCram()) {
                outputBuilder.addReportComponents(new SingleFileComponent(rootBucket,
                                        Aligner.NAMESPACE,
                                        Folder.from(metadata),
                                        bam(metadata.sampleName()),
                                        bam(metadata.sampleName()),
                                        resultsDirectory),
                                new SingleFileComponent(rootBucket,
                                        Aligner.NAMESPACE,
                                        Folder.from(metadata),
                                        bai(bam(metadata.sampleName())),
                                        bai(bam(metadata.sampleName())),
                                        resultsDirectory))
                        .addDatatypes(new AddDatatype(DataType.ALIGNED_READS,
                                        metadata.barcode(),
                                        new ArchivePath(Folder.from(metadata),
                                                BwaAligner.NAMESPACE,
                                                bam(metadata.sampleName()))),
                                new AddDatatype(DataType.ALIGNED_READS_INDEX,
                                        metadata.barcode(),
                                        new ArchivePath(Folder.from(metadata),
                                                BwaAligner.NAMESPACE,
                                                bai(metadata.sampleName()))));
            }
            output = outputBuilder.build();
        } else {
            output = AlignmentOutput.builder().sample(metadata.sampleName()).status(PipelineStatus.FAILED).build();
        }
        trace.stop();
        executorService.shutdown();
        return output;
    }

    public ComputeEngineStatus runWithRetries(final SingleSampleRunMetadata metadata, final RuntimeBucket laneBucket,
            final VirtualMachineJobDefinition jobDefinition) {
        return Failsafe.with(DefaultBackoffPolicy.of(String.format("[%s] stage [%s]",
                metadata.toString(),
                Aligner.NAMESPACE))).get(() -> computeEngine.submit(laneBucket, jobDefinition));
    }

    private static String laneNamespace(final LaneInput lane) {
        return NAMESPACE + "/" + laneId(lane);
    }

    static String laneId(final LaneInput lane) {
        return lane.flowCellId() + "-" + lane.laneNumber();
    }

    private boolean lanesSuccessfullyComplete(final List<Future<ComputeEngineStatus>> futures) {
        return futures.stream().map(BwaAligner::getFuture).noneMatch(status -> status.equals(ComputeEngineStatus.FAILED));
    }

    private RuntimeBucket createRuntimeBucket(String namespace, SingleSampleRunMetadata metadata) {
        var runIdentifier = ArgumentUtil.toRunIdentifier(arguments, metadata);
        var runtimeBucketOptions = RuntimeBucketOptions.builder()
                .namespace(namespace)
                .region(arguments.region())
                .labels(labels.asMap())
                .runIdentifier(runIdentifier)
                .cmek(arguments.cmek())
                .build();
        return RuntimeBucket.from(storage, runtimeBucketOptions);
    }

    private static String fastQFileName(final String sample, final String fullFastQPath) {
        return format("samples/%s/%s", sample, new File(fullFastQPath).getName());
    }

    private static ComputeEngineStatus getFuture(final Future<ComputeEngineStatus> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
