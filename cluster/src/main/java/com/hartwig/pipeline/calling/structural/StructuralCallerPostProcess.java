package com.hartwig.pipeline.calling.structural;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssHardFilter;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssSomaticFilter;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.report.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.startingpoint.PersistedLocations;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class StructuralCallerPostProcess implements Stage<StructuralCallerPostProcessOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "gripss";

    private final InputDownload gridssVcf;
    private final InputDownload gridssVcfIndex;

    private final ResourceFiles resourceFiles;
    private String somaticVcf;
    private String somaticFilteredVcf;

    public StructuralCallerPostProcess(final ResourceFiles resourceFiles, StructuralCallerOutput structuralCallerOutput) {
        this.resourceFiles = resourceFiles;
        gridssVcf = new InputDownload(structuralCallerOutput.unfilteredVcf());
        gridssVcfIndex = new InputDownload(structuralCallerOutput.unfilteredVcfIndex());
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(gridssVcf, gridssVcfIndex);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        String tumorSampleName = metadata.tumor().sampleName();
        GridssSomaticFilter somaticFilter = new GridssSomaticFilter(resourceFiles, gridssVcf.getLocalTargetPath());
        GridssHardFilter passAndPonFilter = new GridssHardFilter();

        SubStageInputOutput somaticOutput = somaticFilter.apply(SubStageInputOutput.empty(tumorSampleName));
        SubStageInputOutput somaticFilteredOutput = passAndPonFilter.apply(somaticOutput);

        somaticVcf = somaticOutput.outputFile().path();
        somaticFilteredVcf = somaticFilteredOutput.outputFile().path();

        return new ArrayList<>(somaticFilteredOutput.bash());
    }

    private static String basename(String filename) {
        return new File(filename).getName();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.structuralCalling(bash, resultsDirectory);
    }

    @Override
    public StructuralCallerPostProcessOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return StructuralCallerPostProcessOutput.builder()
                .status(jobStatus)
                .maybeFilteredVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(somaticFilteredVcf))))
                .maybeFilteredVcfIndex(GoogleStorageLocation.of(bucket.name(),
                        resultsDirectory.path(basename(somaticFilteredVcf + ".tbi"))))
                .maybeFullVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(somaticVcf))))
                .maybeFullVcfIndex(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(somaticVcf + ".tbi"))))
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
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.from(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.from()))

                .build();
    }

    @Override
    public StructuralCallerPostProcessOutput skippedOutput(final SomaticRunMetadata metadata) {
        return StructuralCallerPostProcessOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public StructuralCallerPostProcessOutput persistedOutput(final String persistedBucket, final String persistedRun,
            final SomaticRunMetadata metadata) {

        String somaticFilteredVcf =
                String.format("%s.%s.%s", metadata.tumor().sampleName(), GridssHardFilter.GRIDSS_SOMATIC_FILTERED, OutputFile.GZIPPED_VCF);
        String somaticVcf =
                String.format("%s.%s.%s", metadata.tumor().sampleName(), GridssSomaticFilter.GRIDSS_SOMATIC, OutputFile.GZIPPED_VCF);

        return StructuralCallerPostProcessOutput.builder()
                .status(PipelineStatus.PERSISTED)
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
}
