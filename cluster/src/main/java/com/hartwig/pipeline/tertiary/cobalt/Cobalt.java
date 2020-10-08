package com.hartwig.pipeline.tertiary.cobalt;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.AddDatatypeToFile;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;

public class Cobalt extends TertiaryStage<CobaltOutput> {

    public static final String NAMESPACE = "cobalt";

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public Cobalt(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        super(alignmentPair);
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return Collections.singletonList(new CobaltApplicationCommand(metadata.reference().sampleName(),
                getReferenceBamDownload().getLocalTargetPath(),
                metadata.tumor().sampleName(),
                getTumorBamDownload().getLocalTargetPath(),
                resourceFiles.gcProfileFile()));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.cobalt(bash, resultsDirectory);
    }

    @Override
    public CobaltOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return CobaltOutput.builder()
                .status(jobStatus)
                .maybeOutputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                .addLogs(bucket.get(resultsDirectory.path(RunLogComponent.LOG_FILE)))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory))
                .addFurtherOperations(new AddDatatypeToFile(DataType.TUMOR_READ_DEPTH_RATIO,
                        Folder.root(),
                        namespace(),
                        String.format("%s.cobalt.ratio.pcf", metadata.tumor().sampleName()),
                        metadata.barcode()))
                .build();
    }

    @Override
    public CobaltOutput skippedOutput(final SomaticRunMetadata metadata) {
        return CobaltOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public CobaltOutput persistedOutput(final SomaticRunMetadata metadata) {
        return CobaltOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeOutputDirectory(GoogleStorageLocation.of(metadata.bucket(),
                        persistedDataset.directory(metadata, DataType.TUMOR_READ_DEPTH_RATIO)
                                .orElse(PersistedLocations.pathForSet(metadata.set(), namespace())),
                        true))
                .build();
    }
}
