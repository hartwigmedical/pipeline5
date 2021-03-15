package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssHardFilter;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssSomaticFilter;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
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

public class StructuralCallerPostProcess implements Stage<StructuralCallerPostProcessOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "gripss";

    private final InputDownload gridssVcf;
    private final InputDownload gridssVcfIndex;

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;
    private String somaticVcf;
    private String somaticFilteredVcf;

    public StructuralCallerPostProcess(final ResourceFiles resourceFiles, StructuralCallerOutput structuralCallerOutput,
            final PersistedDataset persistedDataset) {
        this.resourceFiles = resourceFiles;
        gridssVcf = new InputDownload(structuralCallerOutput.unfilteredVcf());
        gridssVcfIndex = new InputDownload(structuralCallerOutput.unfilteredVcfIndex());
        this.persistedDataset = persistedDataset;
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
        String referenceSampleName = metadata.reference().sampleName();
        GridssSomaticFilter somaticFilter =
                new GridssSomaticFilter(resourceFiles, tumorSampleName, referenceSampleName, gridssVcf.getLocalTargetPath());
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
        return VirtualMachineJobDefinition.structuralPostProcessCalling(bash, resultsDirectory);
    }

    @Override
    public StructuralCallerPostProcessOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus,
            final RuntimeBucket bucket, final ResultsDirectory resultsDirectory) {
        return StructuralCallerPostProcessOutput.builder()
                .status(jobStatus)
                .maybeFilteredVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(somaticFilteredVcf))))
                .maybeFilteredVcfIndex(GoogleStorageLocation.of(bucket.name(),
                        FileTypes.tabixIndex(resultsDirectory.path(basename(somaticFilteredVcf)))))
                .maybeFullVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(somaticVcf))))
                .maybeFullVcfIndex(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(somaticVcf + ".tbi"))))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.root(),
                        basename(somaticVcf),
                        basename(somaticVcf),
                        resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.root(),
                        basename(somaticFilteredVcf),
                        basename(somaticFilteredVcf),
                        resultsDirectory))
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.root(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.root()))
                .addDatatypes(new AddDatatype(DataType.STRUCTURAL_VARIANTS_GRIPSS_RECOVERY,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), basename(somaticVcf))),
                        new AddDatatype(DataType.STRUCTURAL_VARIANTS_GRIPSS,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), basename(somaticFilteredVcf))))
                .build();
    }

    @Override
    public StructuralCallerPostProcessOutput skippedOutput(final SomaticRunMetadata metadata) {
        return StructuralCallerPostProcessOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public StructuralCallerPostProcessOutput persistedOutput(final SomaticRunMetadata metadata) {

        GoogleStorageLocation somaticFilteredLocation =
                persistedDataset.path(metadata.tumor().sampleName(), DataType.STRUCTURAL_VARIANTS_GRIPSS)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(),
                                        namespace(),
                                        format("%s.%s.%s",
                                                metadata.tumor().sampleName(),
                                                GridssHardFilter.GRIDSS_SOMATIC_FILTERED,
                                                FileTypes.GZIPPED_VCF))));
        GoogleStorageLocation somaticLocation =
                persistedDataset.path(metadata.tumor().sampleName(), DataType.STRUCTURAL_VARIANTS_GRIPSS_RECOVERY)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(),
                                        namespace(),
                                        format("%s.%s.%s",
                                                metadata.tumor().sampleName(),
                                                GridssSomaticFilter.GRIDSS_SOMATIC,
                                                FileTypes.GZIPPED_VCF))));

        return StructuralCallerPostProcessOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeFilteredVcf(somaticFilteredLocation)
                .maybeFilteredVcfIndex(somaticFilteredLocation.transform(FileTypes::tabixIndex))
                .maybeFullVcf(somaticLocation)
                .maybeFullVcfIndex(somaticLocation.transform(FileTypes::tabixIndex))
                .build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runStructuralCaller();
    }
}
