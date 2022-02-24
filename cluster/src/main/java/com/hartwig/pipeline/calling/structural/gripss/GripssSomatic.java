package com.hartwig.pipeline.calling.structural.gripss;

import static java.lang.String.format;

import java.io.File;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
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
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class GripssSomatic implements Stage<GripssSomaticOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "gripss_somatic";

    private static final String GRIPSS_SOMATIC_FILTERED = ".gripss.filtered.somatic.";
    private static final String GRIPSS_SOMATIC_UNFILTERED = ".gripss.somatic.";

    private final InputDownload gridssVcf;
    private final InputDownload gridssVcfIndex;

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;
    private String somaticUnfilteredVcf;
    private String somaticFilteredVcf;

    public GripssSomatic(final ResourceFiles resourceFiles, StructuralCallerOutput structuralCallerOutput,
            final PersistedDataset persistedDataset) {
        this.resourceFiles = resourceFiles;
        gridssVcf = new InputDownload(structuralCallerOutput.unfilteredVariants());
        gridssVcfIndex = new InputDownload(structuralCallerOutput.unfilteredVariants().transform(FileTypes::tabixIndex));
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
        somaticFilteredVcf = filteredVcf(tumorSampleName);
        somaticUnfilteredVcf = unfilteredVcf(tumorSampleName);
        return Collections.singletonList(new GripssCommand(resourceFiles,
                tumorSampleName,
                referenceSampleName,
                gridssVcf.getLocalTargetPath()));
    }

    private static String unfilteredVcf(final String tumorSampleName) {
        return tumorSampleName + GRIPSS_SOMATIC_UNFILTERED + FileTypes.GZIPPED_VCF;
    }

    private static String filteredVcf(final String referenceSampleName) {
        return referenceSampleName + GRIPSS_SOMATIC_FILTERED + FileTypes.GZIPPED_VCF;
    }

    private static String basename(String filename) {
        return new File(filename).getName();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.gripss("somatic", bash, resultsDirectory);
    }

    @Override
    public GripssSomaticOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return GripssSomaticOutput.builder()
                .status(jobStatus)
                .maybeFilteredVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(somaticFilteredVcf))))
                .maybeUnfilteredVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(somaticUnfilteredVcf))))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.root(),
                        basename(somaticUnfilteredVcf),
                        basename(somaticUnfilteredVcf),
                        resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.root(),
                        basename(somaticFilteredVcf),
                        basename(somaticFilteredVcf),
                        resultsDirectory))
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.root(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.root()))
                .addDatatypes(new AddDatatype(DataType.SOMATIC_STRUCTURAL_VARIANTS_GRIPSS_RECOVERY,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), basename(somaticUnfilteredVcf))),
                        new AddDatatype(DataType.SOMATIC_STRUCTURAL_VARIANTS_GRIPSS,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), basename(somaticFilteredVcf))))
                .build();
    }

    @Override
    public GripssSomaticOutput skippedOutput(final SomaticRunMetadata metadata) {
        return GripssSomaticOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public GripssSomaticOutput persistedOutput(final SomaticRunMetadata metadata) {

        GoogleStorageLocation somaticFilteredLocation =
                persistedDataset.path(metadata.tumor().sampleName(), DataType.SOMATIC_STRUCTURAL_VARIANTS_GRIPSS)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), filteredVcf(metadata.tumor().sampleName()))));
        GoogleStorageLocation somaticLocation =
                persistedDataset.path(metadata.tumor().sampleName(), DataType.SOMATIC_STRUCTURAL_VARIANTS_GRIPSS_RECOVERY)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), unfilteredVcf(metadata.tumor().sampleName()))));

        return GripssSomaticOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeFilteredVariants(somaticFilteredLocation)
                .maybeUnfilteredVariants(somaticLocation)
                .build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runStructuralCaller();
    }
}
