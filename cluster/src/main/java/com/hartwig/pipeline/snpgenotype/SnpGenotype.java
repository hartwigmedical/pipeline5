package com.hartwig.pipeline.snpgenotype;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.output.SingleFileComponent;
import com.hartwig.pipeline.output.StartupScriptComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;

@Namespace(SnpGenotype.NAMESPACE)
public class SnpGenotype implements Stage<SnpGenotypeOutput, SingleSampleRunMetadata> {

    public static final String NAMESPACE = "snp_genotype";

    private static final String OUTPUT_FILENAME = "snp_genotype_output.vcf";
    private static final String OUTPUT_FILENAME_MIP = "snp_genotype_output_mip.vcf";

    private final ResourceFiles resourceFiles;
    private final InputDownloadCommand bamDownload;
    private final InputDownloadCommand baiDownload;

    public SnpGenotype(final ResourceFiles resourceFiles, final AlignmentOutput alignmentOutput) {
        this.resourceFiles = resourceFiles;
        this.bamDownload = new InputDownloadCommand(alignmentOutput.alignments());
        this.baiDownload = new InputDownloadCommand(alignmentOutput.alignments().transform(FileTypes::toAlignmentIndex));
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
    public List<BashCommand> tumorOnlyCommands(final SingleSampleRunMetadata metadata) {
        return snpGenotypeCommands();
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SingleSampleRunMetadata metadata) {
        return snpGenotypeCommands();
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SingleSampleRunMetadata metadata) {
        return snpGenotypeCommands();
    }

    public List<BashCommand> snpGenotypeCommands() {
        final ArrayList<BashCommand> commands = new ArrayList<>();
        // Temporarily we will run both the TaqMan array and MIP sequencing in production and therefor create two genotype outputs
        // One with 26 positions for comparing to ARRAY output and one with 31 for comparing to MIP output
        commands.add(new SnpGenotypeCommand(bamDownload.getLocalTargetPath(),
                resourceFiles.refGenomeFile(),
                resourceFiles.genotypeSnpsDB(),
                format("%s/%s", VmDirectories.OUTPUT, OUTPUT_FILENAME)));
        commands.add(new SnpGenotypeCommand(bamDownload.getLocalTargetPath(),
                resourceFiles.refGenomeFile(),
                resourceFiles.genotypeMipSnpsDB(),
                format("%s/%s", VmDirectories.OUTPUT, OUTPUT_FILENAME_MIP)));
        return commands;
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.snpGenotyping(bash, resultsDirectory);
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
                .addReportComponents(new SingleFileComponent(bucket,
                        NAMESPACE,
                        Folder.from(metadata),
                        OUTPUT_FILENAME_MIP,
                        OUTPUT_FILENAME_MIP,
                        resultsDirectory))
                .build();
    }

    @Override
    public SnpGenotypeOutput skippedOutput(final SingleSampleRunMetadata metadata) {
        return SnpGenotypeOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runSnpGenotyper();
    }
}
