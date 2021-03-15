package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.command.BwaCommand;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.calling.structural.gridss.stage.Driver;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssAnnotation;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.ExportPathCommand;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.report.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class StructuralCaller implements Stage<StructuralCallerOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "gridss";

    private final InputDownload referenceBam;
    private final InputDownload referenceBai;
    private final InputDownload tumorBam;
    private final InputDownload tumorBai;

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;
    private String unfilteredVcf;

    public StructuralCaller(final AlignmentPair pair, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        this.resourceFiles = resourceFiles;
        referenceBam = new InputDownload(pair.reference().finalBamLocation());
        referenceBai = new InputDownload(pair.reference().finalBaiLocation());
        tumorBam = new InputDownload(pair.tumor().finalBamLocation());
        tumorBai = new InputDownload(pair.tumor().finalBaiLocation());
        this.persistedDataset = persistedDataset;
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(referenceBam, referenceBai, tumorBam, tumorBai);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        String referenceSampleName = metadata.reference().sampleName();
        String tumorSampleName = metadata.tumor().sampleName();
        String refBamPath = referenceBam.getLocalTargetPath();
        String tumorBamPath = tumorBam.getLocalTargetPath();
        GridssAnnotation viralAnnotation = new GridssAnnotation(resourceFiles, false);

        Driver driver = new Driver(resourceFiles,
                referenceSampleName,
                tumorSampleName,
                VmDirectories.outputFile(tumorSampleName + ".assembly.bam"),
                refBamPath,
                tumorBamPath);
        SubStageInputOutput unfilteredVcfOutput = driver.andThen(viralAnnotation).apply(SubStageInputOutput.empty(tumorSampleName));
        unfilteredVcf = unfilteredVcfOutput.outputFile().path();

        List<BashCommand> commands = new ArrayList<>();
        commands.add(new ExportPathCommand(new BwaCommand()));
        commands.add(new ExportPathCommand(new SamtoolsCommand()));
        commands.addAll(unfilteredVcfOutput.bash());
        return commands;
    }

    private static String basename(String filename) {
        return new File(filename).getName();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.structuralCalling(bash, resultsDirectory);
    }

    @Override
    public StructuralCallerOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return StructuralCallerOutput.builder()
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
    public StructuralCallerOutput skippedOutput(final SomaticRunMetadata metadata) {
        return StructuralCallerOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public StructuralCallerOutput persistedOutput(final SomaticRunMetadata metadata) {

        GoogleStorageLocation unfilteredVcfLocation =
                persistedDataset.path(metadata.tumor().sampleName(), DataType.STRUCTURAL_VARIANTS_GRIDSS)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(),
                                        namespace(),
                                        format("%s.%s.%s",
                                                metadata.tumor().sampleName(),
                                                GridssAnnotation.GRIDSS_ANNOTATED,
                                                FileTypes.GZIPPED_VCF))));

        return StructuralCallerOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeUnfilteredVcf(unfilteredVcfLocation)
                .maybeUnfilteredVcfIndex(unfilteredVcfLocation.transform(FileTypes::tabixIndex))
                .build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runStructuralCaller();
    }

    private static GoogleStorageLocation resultLocation(final RuntimeBucket bucket, final ResultsDirectory resultsDirectory,
            String filename) {
        return GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(filename)));
    }
}
