package com.hartwig.pipeline.calling.structural;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.command.BwaCommand;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.calling.structural.gridss.stage.Driver;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssAnnotation;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssHardFilter;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssSomaticFilter;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.ExportPathCommand;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.report.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.startingpoint.PersistedLocations;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class StructuralCaller implements Stage<StructuralCallerOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "gridss";

    private final InputDownload referenceBam;
    private final InputDownload referenceBai;
    private final InputDownload tumorBam;
    private final InputDownload tumorBai;

    private final ResourceFiles resourceFiles;
    private String unfilteredVcf;
    private String somaticVcf;
    private String somaticFilteredVcf;

    public StructuralCaller(final AlignmentPair pair, final ResourceFiles resourceFiles) {
        this.resourceFiles = resourceFiles;
        referenceBam = new InputDownload(pair.reference().finalBamLocation());
        referenceBai = new InputDownload(pair.reference().finalBaiLocation());
        tumorBam = new InputDownload(pair.tumor().finalBamLocation());
        tumorBai = new InputDownload(pair.tumor().finalBaiLocation());
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
        String tumorSampleName = metadata.tumor().sampleName();
        String refBamPath = referenceBam.getLocalTargetPath();
        String tumorBamPath = tumorBam.getLocalTargetPath();
        GridssAnnotation viralAnnotation = new GridssAnnotation(resourceFiles, false);

        Driver driver = new Driver(resourceFiles, VmDirectories.outputFile(tumorSampleName + ".assembly.bam"), refBamPath, tumorBamPath);
        SubStageInputOutput unfilteredVcfOutput = driver.andThen(viralAnnotation).apply(SubStageInputOutput.empty(tumorSampleName));

        GridssSomaticFilter somaticFilter = new GridssSomaticFilter(resourceFiles, unfilteredVcfOutput.outputFile().path());
        GridssHardFilter passAndPonFilter = new GridssHardFilter();

        SubStageInputOutput somaticOutput = somaticFilter.apply(unfilteredVcfOutput);
        SubStageInputOutput somaticFilteredOutput = passAndPonFilter.apply(somaticOutput);

        unfilteredVcf = unfilteredVcfOutput.outputFile().path();
        somaticVcf = somaticOutput.outputFile().path();
        somaticFilteredVcf = somaticFilteredOutput.outputFile().path();

        List<BashCommand> commands = new ArrayList<>();
        commands.add(new ExportPathCommand(new BwaCommand()));
        commands.add(new ExportPathCommand(new SamtoolsCommand()));
        commands.addAll(somaticFilteredOutput.bash());
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
                .maybeUnfilteredVcf(resultLocation(bucket, resultsDirectory, unfilteredVcf))
                .maybeUnfilteredVcfIndex(resultLocation(bucket, resultsDirectory, unfilteredVcf + ".tbi"))
                .maybeFilteredVcf(resultLocation(bucket, resultsDirectory, somaticFilteredVcf))
                .maybeFilteredVcfIndex(resultLocation(bucket, resultsDirectory, somaticFilteredVcf + ".tbi"))
                .maybeFullVcf(resultLocation(bucket, resultsDirectory, somaticVcf))
                .maybeFullVcfIndex(resultLocation(bucket, resultsDirectory, somaticVcf + ".tbi"))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        basename(unfilteredVcf),
                        basename(unfilteredVcf),
                        resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        basename(somaticVcf),
                        basename(somaticVcf),
                        resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        basename(somaticFilteredVcf),
                        basename(somaticFilteredVcf),
                        resultsDirectory))
                .addReportComponents(new EntireOutputComponent(bucket,
                        Folder.from(),
                        NAMESPACE,
                        resultsDirectory,
                        s -> !s.contains("working") || s.endsWith("sorted.bam.sv.bam") || s.endsWith("sorted.bam.sv.bai")))
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.from(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.from()))

                .build();
    }

    @Override
    public StructuralCallerOutput skippedOutput(final SomaticRunMetadata metadata) {
        return StructuralCallerOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public StructuralCallerOutput persistedOutput(final String persistedBucket, final String persistedRun,
            final SomaticRunMetadata metadata) {

        String somaticFilteredVcf =
                String.format("%s.%s.%s", metadata.tumor().sampleName(), GridssHardFilter.GRIDSS_SOMATIC_FILTERED, OutputFile.GZIPPED_VCF);
        String somaticVcf =
                String.format("%s.%s.%s", metadata.tumor().sampleName(), GridssSomaticFilter.GRIDSS_SOMATIC, OutputFile.GZIPPED_VCF);
        String unfilteredVcf =
                String.format("%s.%s.%s", metadata.tumor().sampleName(), GridssAnnotation.GRIDSS_ANNOTATED, OutputFile.GZIPPED_VCF);

        return StructuralCallerOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeUnfilteredVcf(GoogleStorageLocation.of(persistedBucket,
                        PersistedLocations.blobForSet(persistedRun, namespace(), unfilteredVcf)))
                .maybeUnfilteredVcfIndex(GoogleStorageLocation.of(persistedBucket,
                        PersistedLocations.blobForSet(persistedRun, namespace(), unfilteredVcf) + ".tbi"))
                .maybeFilteredVcf(GoogleStorageLocation.of(persistedBucket,
                        PersistedLocations.blobForSet(persistedRun, namespace(), somaticFilteredVcf)))
                .maybeFilteredVcfIndex(GoogleStorageLocation.of(persistedBucket,
                        PersistedLocations.blobForSet(persistedRun, namespace(), somaticFilteredVcf) + ".tbi"))
                .maybeFullVcf(GoogleStorageLocation.of(persistedBucket,
                        PersistedLocations.blobForSet(persistedRun, namespace(), somaticVcf)))
                .maybeFullVcfIndex(GoogleStorageLocation.of(persistedBucket,
                        PersistedLocations.blobForSet(persistedRun, namespace(), somaticVcf) + ".tbi"))
                .build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runStructuralCaller();
    }

    private static GoogleStorageLocation resultLocation(final RuntimeBucket bucket, final ResultsDirectory resultsDirectory, String filename) {
        return GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(filename)));
    }
}
