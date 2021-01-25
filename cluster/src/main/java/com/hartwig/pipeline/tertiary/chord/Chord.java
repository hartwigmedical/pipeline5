package com.hartwig.pipeline.tertiary.chord;

import static java.lang.String.format;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

public class Chord implements Stage<ChordOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "chord";

    private final RefGenomeVersion refGenomeVersion;
    private final InputDownload purpleStructuralVcfDownload;
    private final InputDownload purpleSomaticVcfDownload;

    public Chord(final RefGenomeVersion refGenomeVersion, final PurpleOutput purpleOutput) {
        this.refGenomeVersion = refGenomeVersion;
        purpleStructuralVcfDownload = new InputDownload(purpleOutput.structuralVcf());
        purpleSomaticVcfDownload = new InputDownload(purpleOutput.somaticVcf());
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(purpleStructuralVcfDownload, purpleSomaticVcfDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return Collections.singletonList(new ChordExtractSigPredictHRD(metadata.tumor().sampleName(),
                purpleSomaticVcfDownload.getLocalTargetPath(),
                purpleStructuralVcfDownload.getLocalTargetPath(),
                refGenomeVersion));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.chord(bash, resultsDirectory);
    }

    @Override
    public ChordOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return ChordOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addFurtherOperations(new AddDatatype(DataType.CHORD_PREDICTION,
                        Folder.root(),
                        format("%s/%s_chord_prediction.txt", namespace(), metadata.tumor().sampleName()),
                        metadata.barcode()))
                .build();
    }

    @Override
    public ChordOutput skippedOutput(final SomaticRunMetadata metadata) {
        return ChordOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow();
    }
}
