package com.hartwig.pipeline.calling.structural.gripss;

import java.io.File;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
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
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

import org.jetbrains.annotations.NotNull;

public class Gripss implements Stage<GripssOutput, SomaticRunMetadata> {

    private final InputDownload gridssVcf;
    private final InputDownload gridssVcfIndex;

    private final PersistedDataset persistedDataset;
    private String unfilteredVcf;
    private String filteredVcf;
    private final GripssConfiguration gripssConfiguration;

    public Gripss(final StructuralCallerOutput structuralCallerOutput, final PersistedDataset persistedDataset,
            final GripssConfiguration gripssConfiguration) {
        this.gridssVcf = new InputDownload(structuralCallerOutput.unfilteredVariants());
        this.gridssVcfIndex = new InputDownload(structuralCallerOutput.unfilteredVariants().transform(FileTypes::tabixIndex));
        this.persistedDataset = persistedDataset;
        this.gripssConfiguration = gripssConfiguration;
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(gridssVcf, gridssVcfIndex);
    }

    @Override
    public String namespace() {
        return gripssConfiguration.namespace();
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        setFields(metadata);
        return Collections.singletonList(gripssConfiguration.commandBuilder()
                .apply(metadata)
                .inputVcf(gridssVcf.getLocalTargetPath())
                .build());
    }

    private void setFields(final SomaticRunMetadata metadata) {
        filteredVcf = gripssConfiguration.filteredVcf().apply(metadata);
        unfilteredVcf = gripssConfiguration.unfilteredVcf().apply(metadata);
    }

    private static String basename(String filename) {
        return new File(filename).getName();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.gripss(namespace().replace("_", "-"), bash, resultsDirectory);
    }

    @Override
    public GripssOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return GripssOutput.builder(namespace())
                .status(jobStatus)
                .maybeFilteredVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(filteredVcf))))
                .maybeUnfilteredVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(unfilteredVcf))))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        namespace(),
                        Folder.root(),
                        basename(unfilteredVcf),
                        basename(unfilteredVcf),
                        resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        namespace(),
                        Folder.root(),
                        basename(filteredVcf),
                        basename(filteredVcf),
                        resultsDirectory))
                .addReportComponents(new RunLogComponent(bucket, namespace(), Folder.root(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, namespace(), Folder.root()))
                .addDatatypes(new AddDatatype(gripssConfiguration.unfilteredDatatype(),
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), basename(unfilteredVcf))),
                        new AddDatatype(gripssConfiguration.filteredDatatype(),
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), basename(filteredVcf))))
                .build();
    }

    @Override
    public GripssOutput skippedOutput(final SomaticRunMetadata metadata) {
        return GripssOutput.builder(namespace()).status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public GripssOutput persistedOutput(final SomaticRunMetadata metadata) {

        GoogleStorageLocation filteredLocation =
                persistedDataset.path(metadata.tumor().sampleName(), gripssConfiguration.filteredDatatype())
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(),
                                        namespace(),
                                        gripssConfiguration.filteredVcf().apply(metadata))));
        GoogleStorageLocation unfilteredLocation =
                persistedDataset.path(metadata.tumor().sampleName(), gripssConfiguration.unfilteredDatatype())
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(),
                                        namespace(),
                                        gripssConfiguration.unfilteredVcf().apply(metadata))));

        return GripssOutput.builder(namespace())
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
