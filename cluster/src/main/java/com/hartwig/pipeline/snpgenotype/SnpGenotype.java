package com.hartwig.pipeline.snpgenotype;

import static java.lang.String.format;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class SnpGenotype implements Stage<SnpGenotypeOutput, SingleSampleRunMetadata> {

    public static final String NAMESPACE = "snp_genotype";

    private static final String OUTPUT_FILENAME = "snp_genotype_output.vcf";

    private final ResourceFiles resourceFiles;
    private final InputDownload bamDownload;
    private final InputDownload baiDownload;

    public SnpGenotype(final ResourceFiles resourceFiles, final AlignmentOutput alignmentOutput) {
        this.resourceFiles = resourceFiles;
        this.bamDownload = new InputDownload(alignmentOutput.finalBamLocation());
        this.baiDownload = new InputDownload(alignmentOutput.finalBaiLocation());
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(bamDownload, baiDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SingleSampleRunMetadata metadata) {
        return Collections.singletonList(new SnpGenotypeCommand(bamDownload.getLocalTargetPath(),
                resourceFiles.refGenomeFile(),
                resourceFiles.genotypeSnpsDB(),
                format("%s/%s", VmDirectories.OUTPUT, OUTPUT_FILENAME)));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.snpGenotyping(bash, resultsDirectory);
    }

    @Override
    public SnpGenotypeOutput persistedOutput(final SingleSampleRunMetadata metadata) {
        return SnpGenotypeOutput.builder().status(PipelineStatus.PERSISTED).build();
    }

    @Override
    public SnpGenotypeOutput output(final SingleSampleRunMetadata metadata, final PipelineStatus status, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return SnpGenotypeOutput.builder()
                .status(status)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.from(metadata), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.from(metadata)))
                .addReportComponents(new SingleFileComponent(bucket,
                        NAMESPACE,
                        Folder.from(metadata),
                        OUTPUT_FILENAME,
                        OUTPUT_FILENAME,
                        resultsDirectory))
                .build();
    }

    @Override
    public SnpGenotypeOutput skippedOutput(final SingleSampleRunMetadata metadata) {
        return SnpGenotypeOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runSnpGenotyper() && arguments.refGenomeVersion().equals(RefGenomeVersion.HG37);
    }
}
