package com.hartwig.pipeline.tertiary.virusbreakend;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.ExportPathCommand;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;
import com.hartwig.pipeline.tools.Versions;

public class VirusBreakend extends TertiaryStage<VirusBreakendOutput> {

    public static final String NAMESPACE = "virusbreakend";

    public static final String VIRUS_BREAKEND_VCF = ".virusbreakend.vcf";
    public static final String VIRUS_BREAKEND_SUMMARY = ".virusbreakend.vcf.summary.tsv";

    private final ResourceFiles resourceFiles;

    public VirusBreakend(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles) {
        super(alignmentPair);
        this.resourceFiles = resourceFiles;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return List.of(new ExportPathCommand(VmDirectories.toolPath("gridss/" + Versions.VIRUSBREAKEND_GRIDSS)),
                new ExportPathCommand(VmDirectories.toolPath("repeatmasker/" + Versions.REPEAT_MASKER)),
                new ExportPathCommand(VmDirectories.toolPath("kraken2/" + Versions.KRAKEN)),
                new ExportPathCommand(VmDirectories.toolPath("samtools/" + Versions.SAMTOOLS)),
                new ExportPathCommand(VmDirectories.toolPath("bcftools/" + Versions.BCF_TOOLS)),
                new ExportPathCommand(VmDirectories.toolPath("bwa/" + Versions.BWA)),
                new VirusBreakendCommand(resourceFiles, metadata.tumor().sampleName(), getTumorBamDownload().getLocalTargetPath()));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.virusbreakend(bash, resultsDirectory);
    }

    @Override
    public VirusBreakendOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        String vcf = metadata.tumor().sampleName() + VIRUS_BREAKEND_VCF;
        String summary = metadata.tumor().sampleName() + VIRUS_BREAKEND_SUMMARY;
        return VirusBreakendOutput.builder()
                .status(jobStatus)
                .maybeOutputLocations(VirusBreakendOutputLocations.builder()
                        .summaryFile(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(summary)))
                        .build())
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new SingleFileComponent(bucket, NAMESPACE, Folder.root(), vcf, vcf, resultsDirectory),
                        new SingleFileComponent(bucket, NAMESPACE, Folder.root(), summary, summary, resultsDirectory),
                        new RunLogComponent(bucket, NAMESPACE, Folder.root(), resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.VIRUSBREAKEND_VARIANTS,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), vcf)),
                        new AddDatatype(DataType.VIRUSBREAKEND_SUMMARY,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), summary)))
                .build();
    }

    @Override
    public VirusBreakendOutput skippedOutput(final SomaticRunMetadata metadata) {
        return VirusBreakendOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public VirusBreakendOutput persistedOutput(final SomaticRunMetadata metadata) {
        return VirusBreakendOutput.builder().status(PipelineStatus.PERSISTED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary();
    }
}
