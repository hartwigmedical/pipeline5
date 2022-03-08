package com.hartwig.pipeline.calling.structural.gripss;

import java.io.File;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.structural.gridss.GridssOutput;
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

public class GripssGermline implements Stage<GripssGermlineOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "gripss_germline";

    private static final String GRIPSS_GERMLINE_FILTERED = ".gripss.filtered.germline.";
    private static final String GRIPSS_GERMLINE_UNFILTERED = ".gripss.germline.";

    private final InputDownload gridssVcf;
    private final InputDownload gridssVcfIndex;

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;
    private String germlineUnfilteredVcf;
    private String germlineFilteredVcf;

    public GripssGermline(final ResourceFiles resourceFiles, GridssOutput gridssOutput,
            final PersistedDataset persistedDataset) {
        this.resourceFiles = resourceFiles;
        gridssVcf = new InputDownload(gridssOutput.unfilteredVariants());
        gridssVcfIndex = new InputDownload(gridssOutput.unfilteredVariants().transform(FileTypes::tabixIndex));
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
        String referenceSampleName = metadata.reference().sampleName();
        germlineFilteredVcf = filteredVcf(referenceSampleName);
        germlineUnfilteredVcf = unfilteredVcf(referenceSampleName);
        return Collections.singletonList(new GripssCommand(resourceFiles, referenceSampleName, gridssVcf.getLocalTargetPath()));
    }

    private static String unfilteredVcf(final String referenceSampleName) {
        return referenceSampleName + GRIPSS_GERMLINE_UNFILTERED + FileTypes.GZIPPED_VCF;
    }

    private static String filteredVcf(final String referenceSampleName) {
        return referenceSampleName + GRIPSS_GERMLINE_FILTERED + FileTypes.GZIPPED_VCF;
    }

    private static String basename(String filename) {
        return new File(filename).getName();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.gripss("germline", bash, resultsDirectory);
    }

    @Override
    public GripssGermlineOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return GripssGermlineOutput.builder()
                .status(jobStatus)
                .maybeFilteredVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(germlineFilteredVcf))))
                .maybeUnfilteredVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(germlineUnfilteredVcf))))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.root(),
                        basename(germlineUnfilteredVcf),
                        basename(germlineUnfilteredVcf),
                        resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.root(),
                        basename(germlineFilteredVcf),
                        basename(germlineFilteredVcf),
                        resultsDirectory))
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.root(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.root()))
                .addDatatypes(new AddDatatype(DataType.GERMLINE_STRUCTURAL_VARIANTS_GRIPSS_RECOVERY,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), basename(germlineUnfilteredVcf))),
                        new AddDatatype(DataType.GERMLINE_STRUCTURAL_VARIANTS_GRIPSS,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), basename(germlineFilteredVcf))))
                .build();
    }

    @Override
    public GripssGermlineOutput skippedOutput(final SomaticRunMetadata metadata) {
        return GripssGermlineOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public GripssGermlineOutput persistedOutput(final SomaticRunMetadata metadata) {

        GoogleStorageLocation filteredLocation =
                persistedDataset.path(metadata.tumor().sampleName(), DataType.GERMLINE_STRUCTURAL_VARIANTS_GRIPSS)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(),
                                        namespace(),
                                        filteredVcf(metadata.reference().sampleName()))));
        GoogleStorageLocation unfilteredLocation =
                persistedDataset.path(metadata.tumor().sampleName(), DataType.GERMLINE_STRUCTURAL_VARIANTS_GRIPSS_RECOVERY)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(),
                                        namespace(),
                                        unfilteredVcf(metadata.reference().sampleName()))));

        return GripssGermlineOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeFilteredVariants(filteredLocation)
                .maybeUnfilteredVariants(unfilteredLocation)
                .build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runStructuralCaller();
    }
}
