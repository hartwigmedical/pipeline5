package com.hartwig.pipeline.tertiary.lilac;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

public class Lilac extends TertiaryStage<LilacOutput> {

    public static final String NAMESPACE = "lilac";

    private final ResourceFiles resourceFiles;
    private final InputDownload purpleGeneCopyNumberTsv;
    private final InputDownload purpleSomaticVcf;
    private final InputDownload purpleSomaticVcfIndex;

    public Lilac(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles, final PurpleOutput purpleOutput) {
        super(alignmentPair);
        this.resourceFiles = resourceFiles;
        this.purpleGeneCopyNumberTsv = new InputDownload(purpleOutput.outputLocations().geneCopyNumberTsv());
        this.purpleSomaticVcf = new InputDownload(purpleOutput.outputLocations().somaticVcf());
        this.purpleSomaticVcfIndex = new InputDownload(purpleOutput.outputLocations().somaticVcf().transform(x -> x + ".tbi"));
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(purpleGeneCopyNumberTsv, purpleSomaticVcf, purpleSomaticVcfIndex);
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return Collections.singletonList(new LilacApplicationCommand(resourceFiles,
                metadata.tumor().sampleName(),
                getReferenceBamDownload().getLocalTargetPath(),
                getTumorBamDownload().getLocalTargetPath(),
                purpleGeneCopyNumberTsv.getLocalTargetPath(),
                purpleSomaticVcf.getLocalTargetPath()));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.lilac(bash, resultsDirectory);
    }

    @Override
    public LilacOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return LilacOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeOutputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .build();
    }

    @Override
    public LilacOutput skippedOutput(final SomaticRunMetadata metadata) {
        return LilacOutput.builder().status(PipelineStatus.SKIPPED).build();
    }
}
