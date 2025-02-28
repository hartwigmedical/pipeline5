package com.hartwig.pipeline.alignment.bwa;

import static java.lang.String.format;

import static com.hartwig.pipeline.alignment.redux.Redux.jitterParamsTsv;
import static com.hartwig.pipeline.alignment.redux.Redux.msTableTsv;
import static com.hartwig.pipeline.datatypes.FileTypes.bai;
import static com.hartwig.pipeline.datatypes.FileTypes.bam;
import static com.hartwig.pipeline.resource.ResourceFilesFactory.buildResourceFiles;

import java.io.File;
import java.io.IOException;
import java.net.URI;
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
import com.hartwig.gcp.StorageUtil;
import com.hartwig.pdl.LaneInput;
import com.hartwig.pdl.PipelineInput;
import com.hartwig.pdl.SampleInput;
import com.hartwig.pipeline.ArgumentUtil;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.ImmutableAlignmentOutput;
import com.hartwig.pipeline.alignment.redux.Redux;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
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
import com.hartwig.pipeline.reference.api.Pipeline;
import com.hartwig.pipeline.reference.api.PipelineFiles;
import com.hartwig.pipeline.reference.api.PipelineOutputStructure;
import com.hartwig.pipeline.reference.api.PipelineOutputTemporaryLocation;
import com.hartwig.pipeline.reference.api.PipelineRun;
import com.hartwig.pipeline.reference.api.SampleType;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.SampleUpload;
import com.hartwig.pipeline.trace.StageTrace;

import net.jodah.failsafe.Failsafe;

public class BwaAligner implements Aligner {

    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final StorageUtil storageUtil;
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
        this.storageUtil = new StorageUtil(storage);
        this.input = input;
        this.sampleUpload = sampleUpload;
        this.resultsDirectory = resultsDirectory;
        this.executorService = executorService;
        this.labels = labels;
    }

    public AlignmentOutput run(final SingleSampleRunMetadata metadata) throws Exception {
        StageTrace trace = new StageTrace(NAMESPACE, metadata.sampleName(), StageTrace.ExecutorType.COMPUTE_ENGINE).start();
        RuntimeBucket rootBucket = createRuntimeBucket(NAMESPACE, metadata);
        SampleInput sample = Inputs.sampleFor(input, metadata);

        if (sample.bam().isPresent() && !arguments.redoDuplicateMarking()) {
            cleanUp(trace);
            var bamLocation = sample.bam().get();
            return AlignmentOutput.builder()
                    .sample(metadata.sampleName())
                    .status(PipelineStatus.PROVIDED)
                    .maybeAlignments(GoogleStorageLocation.from(bamLocation, arguments.project()))
                    .maybeJitterParams(locateJitterParamsFile(metadata, bamLocation))
                    .maybeMsTable(locateMsTableFile(metadata, bamLocation))
                    .build();
        }

        final ResourceFiles resourceFiles = buildResourceFiles(arguments);

        AlignmentStatus alignmentStatus;
        boolean alignmentSuccessful;
        List<GoogleStorageLocation> unmergedBams;
        List<GoogleStorageLocation> laneAlignmentLogs;
        List<OutputComponent> laneAlignmentOutputComponents;
        if (sample.bam().isEmpty()) {
            alignmentStatus = submitLaneAlignments(sample, rootBucket, metadata, resourceFiles);

            alignmentSuccessful = alignmentStatus.alignmentSuccessfullyCompleted();
            unmergedBams = alignmentStatus.bamLocations;
            laneAlignmentLogs = alignmentStatus.logLocations;
            laneAlignmentOutputComponents = alignmentStatus.runLogComponents;
        } else {
            // For redoing duplicate marking without redoing alignment
            alignmentSuccessful = true;
            unmergedBams = List.of(GoogleStorageLocation.from(sample.bam().get(), arguments.project()));
            laneAlignmentLogs = new ArrayList<>();
            laneAlignmentOutputComponents = new ArrayList<>();
        }

        AlignmentOutput output;
        if (alignmentSuccessful) {
            output = runRedux(unmergedBams, rootBucket, metadata, resourceFiles, laneAlignmentLogs, laneAlignmentOutputComponents);
        } else {
            output = AlignmentOutput.builder().sample(metadata.sampleName()).status(PipelineStatus.FAILED).build();
        }
        cleanUp(trace);
        return output;
    }

    private GoogleStorageLocation locateJitterParamsFile(SingleSampleRunMetadata metadata, String bamLocation) {
        return locateFile(metadata, bamLocation, com.hartwig.pipeline.reference.api.DataType.REDUX_JITTER_PARAMS);
    }

    private GoogleStorageLocation locateMsTableFile(SingleSampleRunMetadata metadata, String bamLocation) {
        return locateFile(metadata, bamLocation, com.hartwig.pipeline.reference.api.DataType.REDUX_MS_TABLE);
    }

    private GoogleStorageLocation locateFile(SingleSampleRunMetadata metadata, String bamLocation,
            com.hartwig.pipeline.reference.api.DataType dataType) {
        // Because the tumor and reference are not nullable, but they are not necessary for location the file, we use the placeholder ___
        var tumor = metadata.type().equals(SingleSampleRunMetadata.SampleType.TUMOR) ? metadata.sampleName() : "___";
        var reference = metadata.type().equals(SingleSampleRunMetadata.SampleType.REFERENCE) ? metadata.sampleName() : "___";
        var pipelineRun = new PipelineRun(Pipeline.DNA_6_0, tumor, reference);
        var refDataSampleType = metadata.type().equals(SingleSampleRunMetadata.SampleType.TUMOR) ? SampleType.TUMOR : SampleType.REFERENCE;
        // We assume the default output format for pipeline5, both the BAM and the CRAM file are 2 levels below the pipeline output root.
        var pipelineOutputLocation =
                new PipelineOutputTemporaryLocation(URI.create(bamLocation).resolve("../../"), PipelineOutputStructure.PIPELINE5);

        var reduxFile =
                PipelineFiles.get(pipelineRun, PipelineFiles.sampleTypeIs(refDataSampleType), PipelineFiles.dataTypeIsAnyOf(dataType))
                        .stream()
                        .findFirst()
                        .map(it -> it.getUriOrNull(pipelineOutputLocation))
                        .map(URI::toString)
                        .orElseThrow();
        if (!storageUtil.exists(reduxFile)) {
            // If the file is not found, it either means the user made a mistake, or the file really does not exist.
            // In the first case, we let the user know by crashing pipeline5.
            // In the second case, the user should redo marking duplicates to generate the file.
            throw new IllegalStateException(("Duplicate marking output file not found. Expected at location: '%s'. "
                                             + "If this is intentional, consider enabling the '--redo_duplicate_marking' flag.").formatted(
                    reduxFile));
        }
        return GoogleStorageLocation.from(reduxFile, arguments.project());
    }

    private AlignmentStatus submitLaneAlignments(final SampleInput sample, final RuntimeBucket rootBucket,
            final SingleSampleRunMetadata metadata, final ResourceFiles resourceFiles) throws IOException {
        sampleUpload.run(sample, rootBucket);

        List<AlignmentStatus> laneAlignmentStatuses = new ArrayList<>();
        for (LaneInput lane : sample.lanes()) {
            RuntimeBucket laneBucket = createRuntimeBucket(laneNamespace(lane), metadata);
            AlignmentStatus laneAlignmentStatus = submitLaneAlignment(lane, sample, laneBucket, rootBucket, metadata, resourceFiles);
            laneAlignmentStatuses.add(laneAlignmentStatus);
        }
        return AlignmentStatus.combine(laneAlignmentStatuses);
    }

    private AlignmentStatus submitLaneAlignment(final LaneInput lane, final SampleInput sample, final RuntimeBucket laneBucket,
            final RuntimeBucket rootBucket, final SingleSampleRunMetadata metadata, final ResourceFiles resourceFiles) {
        InputDownloadCommand firstDownload = new InputDownloadCommand(GoogleStorageLocation.of(rootBucket.name(),
                fastQFileName(sample.name(), lane.firstOfPairPath())));
        InputDownloadCommand secondDownload = new InputDownloadCommand(GoogleStorageLocation.of(rootBucket.name(),
                fastQFileName(sample.name(), lane.secondOfPairPath())));
        SubStageInputOutput alignment = new LaneAlignment(arguments.sbpApiRunId().isPresent(),
                resourceFiles.refGenomeFile(),
                firstDownload.getLocalTargetPath(),
                secondDownload.getLocalTargetPath(), lane).apply(SubStageInputOutput.empty(metadata.sampleName()));
        OutputUploadCommand outputUpload =
                new OutputUploadCommand(GoogleStorageLocation.of(laneBucket.name(), resultsDirectory.path()),
                        RuntimeFiles.typical());

        BashStartupScript laneBash = BashStartupScript.of(laneBucket.name());
        laneBash.addCommand(firstDownload)
                .addCommand(secondDownload)
                .addCommands(alignment.bash())
                .addCommand(outputUpload);

        var pipelineFuture = executorService.submit(() -> runWithRetries(metadata, laneBucket,
                VirtualMachineJobDefinitions.alignment(laneBash, resultsDirectory, "aligner-" + laneId(lane).toLowerCase())));

        GoogleStorageLocation bamLocation = GoogleStorageLocation.of(laneBucket.name(), resultsDirectory.path(alignment.outputFile().fileName()));
        OutputComponent runLogComponent = new RunLogComponent(laneBucket, laneNamespace(lane), Folder.from(metadata), resultsDirectory);
        GoogleStorageLocation logLocation = GoogleStorageLocation.of(laneBucket.name(), RunLogComponent.LOG_FILE);

        return new AlignmentStatus(List.of(pipelineFuture), List.of(bamLocation), List.of(logLocation), List.of(runLogComponent));
    }

    private AlignmentOutput runRedux(final List<GoogleStorageLocation> unmergedBams, final RuntimeBucket rootBucket,
            final SingleSampleRunMetadata metadata, final ResourceFiles resourceFiles, final List<GoogleStorageLocation> laneAlignmentLogs,
            final List<OutputComponent> laneAlignmentOutputComponents) {

        List<InputDownloadCommand> unmergedBamDownloads =
                unmergedBams.stream().map(InputDownloadCommand::new).collect(Collectors.toList());
        List<InputDownloadCommand> unmergedBamIndexDownloads = unmergedBams.stream()
                .map(x -> x.transform(FileTypes::toAlignmentIndex))
                .map(InputDownloadCommand::new)
                .collect(Collectors.toList());

        List<String> localBamPaths = unmergedBamDownloads.stream()
                .map(InputDownloadCommand::getLocalTargetPath)
                .collect(Collectors.toList());
        SubStageInputOutput redux =
                new Redux(metadata.sampleName(), resourceFiles, localBamPaths).apply(SubStageInputOutput.empty(metadata.sampleName()));

        OutputUploadCommand outputUpload =
                new OutputUploadCommand(GoogleStorageLocation.of(rootBucket.name(), resultsDirectory.path()), RuntimeFiles.typical());

        BashStartupScript mergeMarkdupsBash = BashStartupScript.of(rootBucket.name());
        mergeMarkdupsBash.addCommands(unmergedBamDownloads)
                .addCommands(unmergedBamIndexDownloads)
                .addCommands(redux.bash())
                .addCommand(outputUpload);

        ComputeEngineStatus computeEngineStatus =
                runWithRetries(metadata, rootBucket, VirtualMachineJobDefinitions.mergeMarkdups(mergeMarkdupsBash, resultsDirectory));

        String jitterParams = resultsDirectory.path(jitterParamsTsv(metadata.sampleName()));
        String msTableParams = resultsDirectory.path(msTableTsv(metadata.sampleName()));
        ImmutableAlignmentOutput.Builder outputBuilder = AlignmentOutput.builder()
                .sample(metadata.sampleName())
                .status(PipelineStatus.of(computeEngineStatus))
                .maybeAlignments(GoogleStorageLocation.of(rootBucket.name(), resultsDirectory.path(redux.outputFile().fileName())))
                .maybeJitterParams(GoogleStorageLocation.of(rootBucket.name(), jitterParams))
                .maybeMsTable(GoogleStorageLocation.of(rootBucket.name(), msTableParams))
                .addAllFailedLogLocations(laneAlignmentLogs)
                .addAllReportComponents(laneAlignmentOutputComponents)
                .addFailedLogLocations(GoogleStorageLocation.of(rootBucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new RunLogComponent(rootBucket, Aligner.NAMESPACE, Folder.from(metadata), resultsDirectory),
                        new SingleFileComponent(rootBucket,
                                Aligner.NAMESPACE,
                                Folder.from(metadata),
                                jitterParamsTsv(metadata.sampleName()),
                                jitterParamsTsv(metadata.sampleName()),
                                resultsDirectory),
                        new SingleFileComponent(rootBucket,
                                Aligner.NAMESPACE,
                                Folder.from(metadata),
                                msTableTsv(metadata.sampleName()),
                                msTableTsv(metadata.sampleName()),
                                resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.REDUX_JITTER_PARAMS,
                                metadata.barcode(),
                                new ArchivePath(Folder.from(metadata), BwaAligner.NAMESPACE, jitterParamsTsv(metadata.sampleName()))),
                        new AddDatatype(DataType.REDUX_MS_TABLE,
                                metadata.barcode(),
                                new ArchivePath(Folder.from(metadata), BwaAligner.NAMESPACE, msTableTsv(metadata.sampleName()))));

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
                            resultsDirectory));
            outputBuilder.addDatatypes(new AddDatatype(DataType.ALIGNED_READS,
                            metadata.barcode(),
                            new ArchivePath(Folder.from(metadata), BwaAligner.NAMESPACE, bam(metadata.sampleName()))),
                    new AddDatatype(DataType.ALIGNED_READS_INDEX,
                            metadata.barcode(),
                            new ArchivePath(Folder.from(metadata), BwaAligner.NAMESPACE, bai(bam(metadata.sampleName())))));
        }
        return outputBuilder.build();
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

    private void cleanUp(final StageTrace trace) {
        trace.stop();
        executorService.shutdown();
    }

    private static class AlignmentStatus {
        private final List<Future<ComputeEngineStatus>> pipelineFutures;
        public final List<GoogleStorageLocation> bamLocations;
        public final List<GoogleStorageLocation> logLocations;
        public final List<OutputComponent> runLogComponents;

        public AlignmentStatus(final List<Future<ComputeEngineStatus>> pipelineFutures, final List<GoogleStorageLocation> bamLocations,
                final List<GoogleStorageLocation> logLocations, final List<OutputComponent> runLogComponents) {
            this.pipelineFutures = pipelineFutures;
            this.bamLocations = bamLocations;
            this.logLocations = logLocations;
            this.runLogComponents = runLogComponents;
        }

        public static AlignmentStatus combine(List<AlignmentStatus> outputs) {

            return new AlignmentStatus(
                    outputs.stream().flatMap(o -> o.pipelineFutures.stream()).collect(Collectors.toList()),
                    outputs.stream().flatMap(o -> o.bamLocations.stream()).collect(Collectors.toList()),
                    outputs.stream().flatMap(o -> o.logLocations.stream()).collect(Collectors.toList()),
                    outputs.stream().flatMap(o -> o.runLogComponents.stream()).collect(Collectors.toList())
            );
        }

        public boolean alignmentSuccessfullyCompleted() {
            return pipelineFutures.stream()
                    .map(AlignmentStatus::getFuture)
                    .noneMatch(status -> status.equals(ComputeEngineStatus.FAILED));
        }

        private static ComputeEngineStatus getFuture(final Future<ComputeEngineStatus> future) {
            try {
                return future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
