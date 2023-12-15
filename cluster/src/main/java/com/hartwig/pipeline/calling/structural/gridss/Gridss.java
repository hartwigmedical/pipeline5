package com.hartwig.pipeline.calling.structural.gridss;

import static java.lang.String.format;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.command.BwaCommand;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssAnnotation;
import com.hartwig.pipeline.calling.structural.gridss.stage.SvCalling;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.unix.ExportPathCommand;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.EntireOutputComponent;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.output.StartupScriptComponent;
import com.hartwig.pipeline.output.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;

@Namespace(Gridss.NAMESPACE)
public class Gridss extends TertiaryStage<GridssOutput> {
    public static final String NAMESPACE = "gridss";

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;
    private String unfilteredVcf;

    public Gridss(final AlignmentPair pair, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        super(pair);
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        String tumorSampleName = metadata.tumor().sampleName();
        String tumorBamPath = getTumorBamDownload().getLocalTargetPath();
        SvCalling svCalling = new SvCalling(resourceFiles).tumorSample(tumorSampleName, tumorBamPath);
        return gridssCommands(svCalling, tumorSampleName);
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        String referenceSampleName = metadata.reference().sampleName();
        String refBamPath = getReferenceBamDownload().getLocalTargetPath();
        SvCalling svCalling = new SvCalling(resourceFiles).referenceSample(referenceSampleName, refBamPath);
        return gridssCommands(svCalling, referenceSampleName);
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        String referenceSampleName = metadata.reference().sampleName();
        String tumorSampleName = metadata.tumor().sampleName();
        String refBamPath = getReferenceBamDownload().getLocalTargetPath();
        String tumorBamPath = getTumorBamDownload().getLocalTargetPath();
        return gridssCommands(new SvCalling(resourceFiles).tumorSample(tumorSampleName, tumorBamPath)
                .referenceSample(referenceSampleName, refBamPath), tumorSampleName);
    }

    private List<BashCommand> gridssCommands(final SvCalling svCalling, final String sampleName) {
        SubStageInputOutput unfilteredVcfOutput =
                svCalling.andThen(new GridssAnnotation(resourceFiles)).apply(SubStageInputOutput.empty(sampleName));
        unfilteredVcf = unfilteredVcfOutput.outputFile().path();

        List<BashCommand> commands = new ArrayList<>();
        commands.add(new ExportPathCommand(new BwaCommand()));
        commands.add(new ExportPathCommand(new SamtoolsCommand()));
        commands.addAll(unfilteredVcfOutput.bash());
        return commands;
    }

    private static String basename(final String filename) {
        return new File(filename).getName();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.structuralCalling(bash, resultsDirectory);
    }

    @Override
    public GridssOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return GridssOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeUnfilteredVcf(resultLocation(bucket, resultsDirectory, unfilteredVcf))
                .maybeUnfilteredVcfIndex(resultLocation(bucket, resultsDirectory, unfilteredVcf + ".tbi"))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.root(),
                        basename(unfilteredVcf),
                        basename(unfilteredVcf),
                        resultsDirectory))
                .addReportComponents(new EntireOutputComponent(bucket,
                        Folder.root(),
                        NAMESPACE,
                        resultsDirectory,
                        s -> !s.contains("working") || s.endsWith("bam.sv.bam") || s.endsWith("bam.sv.bam.bai")))
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.root(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.root()))
                .addDatatypes(new AddDatatype(DataType.STRUCTURAL_VARIANTS_GRIDSS,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), basename(unfilteredVcf))))
                .build();
    }

    @Override
    public GridssOutput skippedOutput(final SomaticRunMetadata metadata) {
        return GridssOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public GridssOutput persistedOutput(final SomaticRunMetadata metadata) {

        GoogleStorageLocation unfilteredVcfLocation =
                persistedDataset.path(metadata.tumor().sampleName(), DataType.STRUCTURAL_VARIANTS_GRIDSS)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(),
                                        namespace(),
                                        format("%s.%s.%s",
                                                metadata.tumor().sampleName(),
                                                GridssAnnotation.GRIDSS_ANNOTATED,
                                                FileTypes.GZIPPED_VCF))));

        return GridssOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeUnfilteredVcf(unfilteredVcfLocation)
                .maybeUnfilteredVcfIndex(unfilteredVcfLocation.transform(FileTypes::tabixIndex))
                .build();
    }

    private static GoogleStorageLocation resultLocation(final RuntimeBucket bucket, final ResultsDirectory resultsDirectory,
            final String filename) {
        return GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(filename)));
    }
}
