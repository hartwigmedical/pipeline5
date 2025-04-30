package com.hartwig.pipeline.cram;

import static com.hartwig.pipeline.tools.ExternalTool.SAMTOOLS;

import java.io.File;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.execution.vm.Bash;
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
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.JavaCommandFactory;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.input.SingleSampleRunMetadata.SampleType;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.output.SingleFileComponent;
import com.hartwig.pipeline.output.StartupScriptComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tools.HmfTool;

@Namespace(CramConversion.NAMESPACE)
public class CramConversion implements Stage<CramOutput, SingleSampleRunMetadata> {
    public static final String NAMESPACE = "cram";

    public static final int NUMBER_OF_CORES = 16;
    public static final int MEMORY_GB = 16;

    private final InputDownloadCommand bamDownload;
    private final String outputCram;
    private final String bamCompareTsv;
    private final SampleType sampleType;
    private final ResourceFiles resourceFiles;

    public CramConversion(final AlignmentOutput alignmentOutput, final SampleType sampleType, final ResourceFiles resourceFiles) {
        bamDownload = new InputDownloadCommand(alignmentOutput.alignments());
        var sample = alignmentOutput.sample();
        outputCram = VmDirectories.outputFile(FileTypes.cram(sample));
        bamCompareTsv = VmDirectories.outputFile(sample + ".bam_compare.tsv");
        this.sampleType = sampleType;
        this.resourceFiles = resourceFiles;
    }

    @Override
    public List<BashCommand> inputs() {
        return Collections.singletonList(bamDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SingleSampleRunMetadata metadata) {
        return cramCommands();
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SingleSampleRunMetadata metadata) {
        return cramCommands();
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SingleSampleRunMetadata metadata) {
        return cramCommands();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.cramConversion(bash, resultsDirectory, sampleType);
    }

    @Override
    public CramOutput output(final SingleSampleRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        String cram = cram();
        String crai = FileTypes.crai(cram);
        Folder folder = Folder.from(metadata);

        return CramOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, folder, resultsDirectory),
                        new StartupScriptComponent(bucket, NAMESPACE, folder),
                        new SingleFileComponent(bucket, NAMESPACE, folder, cram, cram, resultsDirectory),
                        new SingleFileComponent(bucket, NAMESPACE, folder, crai, crai, resultsDirectory))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    private String cram() {
        return new File(outputCram).getName();
    }

    @Override
    public CramOutput skippedOutput(final SingleSampleRunMetadata metadata) {
        return CramOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SingleSampleRunMetadata metadata) {
        return List.of(new AddDatatype(DataType.ALIGNED_READS,
                        metadata.barcode(),
                        new ArchivePath(Folder.from(metadata), namespace(), cram())),
                new AddDatatype(DataType.ALIGNED_READS_INDEX,
                        metadata.barcode(),
                        new ArchivePath(Folder.from(metadata), namespace(), FileTypes.crai(cram()))));
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.outputCram();
    }

    private List<BashCommand> cramCommands() {
        var inputBam = bamDownload.getLocalTargetPath();
        var samtoolsView = new VersionedToolCommand(SAMTOOLS.getToolName(),
                SAMTOOLS.getBinary(),
                SAMTOOLS.getVersion(),
                "view",
                "-T",
                resourceFiles.refGenomeFile(),
                "-o",
                outputCram,
                "-O",
                "cram,embed_ref=1",
                "-@",
                Bash.allCpus(),
                inputBam);
        var samtoolsReheader = new VersionedToolCommand(SAMTOOLS.getToolName(),
                SAMTOOLS.getBinary(),
                SAMTOOLS.getVersion(),
                "reheader",
                "--no-PG",
                "--in-place",
                "--command",
                "'grep -v ^@PG'",
                outputCram);
        var samtoolsIndex =
                new VersionedToolCommand(SAMTOOLS.getToolName(), SAMTOOLS.getBinary(), SAMTOOLS.getVersion(), "index", outputCram);
        var bamCompare = JavaCommandFactory.javaClassCommand(HmfTool.BAM_TOOLS,
                "com.hartwig.hmftools.bamtools.compare.BamCompare",
                List.of("-orig_bam_file",
                        inputBam,
                        "-new_bam_file",
                        outputCram,
                        "-ref_genome",
                        resourceFiles.refGenomeFile(),
                        "-threads",
                        Bash.allCpus(),
                        "-output_file", bamCompareTsv));
        return ImmutableList.of(samtoolsView, samtoolsReheader, samtoolsIndex, bamCompare);
    }
}
