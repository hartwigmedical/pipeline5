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
import com.hartwig.pipeline.execution.vm.unix.ExportPathCommand;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;
import com.hartwig.pipeline.tools.Versions;

public class VirusBreakend extends TertiaryStage<VirusBreakendOutput> {

    public static final String NAMESPACE = "virusbreakend";

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
        return List.of(
                new ExportPathCommand("/opt/tools/gridss/"+ Versions.GRIDSS),
                new ExportPathCommand("/opt/tools/repeatmasker/4.1.1"),
                new ExportPathCommand("/opt/tools/kraken2/2.1.0"),
                new ExportPathCommand("/opt/tools/samtools/1.10"),
                new ExportPathCommand("/opt/tools/bcftools/1.9"),
                new ExportPathCommand("/opt/tools/bwa/0.7.17"),
                new VirusBreakendCommand(resourceFiles,
                metadata.tumor().sampleName(),
                getTumorBamDownload().getLocalTargetPath()));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.virusbreakend(bash, resultsDirectory);
    }

    @Override
    public VirusBreakendOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return VirusBreakendOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addDatatypes(
                        new AddDatatype(DataType.VIRUSBREAKEND_VARIANTS,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), String.format("%s.virusbreakend.vcf", metadata.tumor().sampleName()))),
                        new AddDatatype(DataType.VIRUSBREAKEND_SUMMARY,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), String.format("%s.virusbreakend.summary.tsv", metadata.tumor().sampleName())))
                )
                .build();
    }

    @Override
    public VirusBreakendOutput skippedOutput(final SomaticRunMetadata metadata) {
        return VirusBreakendOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public VirusBreakendOutput persistedOutput(final SomaticRunMetadata metadata) {
        return VirusBreakendOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary();
    }
}
