package com.hartwig.pipeline.calling.structural.gripss;

import static java.lang.String.format;

import java.io.File;
import java.util.ArrayList;
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
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class GripssGermlineProcess implements Stage<GripssGermlineProcessOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "gripss_germline";

    private final InputDownload gridssVcf;
    private final InputDownload gridssVcfIndex;

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;
    private String germlineUnfilteredVcf;
    private String germlineFilteredVcf;

    public GripssGermlineProcess(final ResourceFiles resourceFiles, StructuralCallerOutput structuralCallerOutput,
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
        String referenceSampleName = metadata.reference().sampleName();

        GripssGermline gripssGermline = new GripssGermline(resourceFiles, referenceSampleName, gridssVcf.getLocalTargetPath());

        SubStageInputOutput gripssOutput = gripssGermline.apply(SubStageInputOutput.empty(referenceSampleName));
        germlineFilteredVcf = gripssOutput.outputFile().path();
        germlineUnfilteredVcf = gripssGermline.unfilteredVcf(referenceSampleName);

        return new ArrayList<>(gripssOutput.bash());
    }

    private static String basename(String filename) {
        return new File(filename).getName();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.gripssGermline(bash, resultsDirectory);
    }

    @Override
    public GripssGermlineProcessOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus,
            final RuntimeBucket bucket, final ResultsDirectory resultsDirectory) {
        return GripssGermlineProcessOutput.builder()
                .status(jobStatus)
                .maybeFilteredVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(germlineFilteredVcf))))
                .maybeFilteredVcfIndex(GoogleStorageLocation.of(bucket.name(),
                        FileTypes.tabixIndex(resultsDirectory.path(basename(germlineFilteredVcf)))))
                .maybeFullVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(germlineUnfilteredVcf))))
                .maybeFullVcfIndex(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(germlineUnfilteredVcf + ".tbi"))))
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
    public GripssGermlineProcessOutput skippedOutput(final SomaticRunMetadata metadata) {
        return GripssGermlineProcessOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public GripssGermlineProcessOutput persistedOutput(final SomaticRunMetadata metadata) {

        GoogleStorageLocation filteredLocation =
                persistedDataset.path(metadata.tumor().sampleName(), DataType.GERMLINE_STRUCTURAL_VARIANTS_GRIPSS)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(),
                                        namespace(),
                                        format("%s.%s.%s",
                                                metadata.tumor().sampleName(),
                                                GripssGermline.GRIPSS_GERMLINE_FILTERED,
                                                FileTypes.GZIPPED_VCF))));
        GoogleStorageLocation unfilteredLocation =
                persistedDataset.path(metadata.tumor().sampleName(), DataType.GERMLINE_STRUCTURAL_VARIANTS_GRIPSS_RECOVERY)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(),
                                        namespace(),
                                        format("%s.%s.%s",
                                                metadata.tumor().sampleName(),
                                                GripssGermline.GRIPSS_GERMLINE_UNFILTERED,
                                                FileTypes.GZIPPED_VCF))));

        return GripssGermlineProcessOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeFilteredVcf(filteredLocation)
                .maybeFilteredVcfIndex(filteredLocation.transform(FileTypes::tabixIndex))
                .maybeFullVcf(unfilteredLocation)
                .maybeFullVcfIndex(unfilteredLocation.transform(FileTypes::tabixIndex))
                .build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runStructuralCaller();
    }
}
