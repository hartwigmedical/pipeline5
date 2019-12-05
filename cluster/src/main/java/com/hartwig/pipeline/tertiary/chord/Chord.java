package com.hartwig.pipeline.tertiary.chord;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

public class Chord implements Stage<ChordOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "chord";

    private final InputDownload purpleStructuralVcfDownload;
    private final InputDownload purpleSomaticVcfDownload;

    public Chord(final PurpleOutput purpleOutput) {
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
                purpleStructuralVcfDownload.getLocalTargetPath()));
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
                .addReportComponents(new EntireOutputComponent(bucket, Folder.from(), Chord.NAMESPACE, resultsDirectory))
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
