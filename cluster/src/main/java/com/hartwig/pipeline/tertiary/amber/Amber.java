package com.hartwig.pipeline.tertiary.amber;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.Arguments;
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
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;

public class Amber extends TertiaryStage<AmberOutput> {

    public static final String NAMESPACE = "amber";

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public Amber(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
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
        return Collections.singletonList(new AmberApplicationCommand(metadata.reference().sampleName(),
                getReferenceBamDownload().getLocalTargetPath(),
                metadata.tumor().sampleName(),
                getTumorBamDownload().getLocalTargetPath(),
                resourceFiles.refGenomeFile(),
                resourceFiles.amberHeterozygousLoci()));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.amber(bash, resultsDirectory);
    }

    @Override
    public AmberOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return AmberOutput.builder()
                .status(jobStatus)
                .maybeOutputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory))
                .addFurtherOperations(new AddDatatypeToFile(DataType.B_ALLELE_FREQUENCY,
                        Folder.root(),
                        namespace(),
                        String.format("%s.amber.baf.pcf", metadata.tumor().sampleName()),
                        metadata.barcode()))
                .build();
    }

    @Override
    public AmberOutput skippedOutput(final SomaticRunMetadata metadata) {
        return AmberOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public AmberOutput persistedOutput(final SomaticRunMetadata metadata) {
        return AmberOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeOutputDirectory(GoogleStorageLocation.of(metadata.bucket(),
                        persistedDataset.directory(metadata, DataType.B_ALLELE_FREQUENCY)
                                .orElse(PersistedLocations.pathForSet(metadata.set(), namespace())),
                        true))
                .build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary();
    }
}
