package com.hartwig.pipeline.tertiary.virus;

import java.util.ArrayList;
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
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;
import com.hartwig.pipeline.tools.Versions;

@Namespace(VirusBreakend.NAMESPACE)
public class VirusBreakend extends TertiaryStage<VirusBreakendOutput> {

    public static final String NAMESPACE = "virusbreakend";

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public VirusBreakend(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        super(alignmentPair);
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public String namespace() {
        return VirusBreakend.NAMESPACE;
    }

    @Override
    public List<BashCommand> inputs() {
        return new ArrayList<>(super.inputs());
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.virusbreakend(startupScript, resultsDirectory);
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        return commands(metadata);
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        return commands(metadata);
    }

    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        String tumorSample = metadata.tumor().sampleName();
        return List.of(new ExportPathCommand(VmDirectories.toolPath("gridss/" + Versions.VIRUSBREAKEND_GRIDSS)),
                new ExportPathCommand(VmDirectories.toolPath("repeatmasker/" + Versions.REPEAT_MASKER)),
                new ExportPathCommand(VmDirectories.toolPath("kraken2/" + Versions.KRAKEN)),
                new ExportPathCommand(VmDirectories.toolPath("samtools/" + Versions.SAMTOOLS)),
                new ExportPathCommand(VmDirectories.toolPath("bcftools/" + Versions.BCF_TOOLS)),
                new ExportPathCommand(VmDirectories.toolPath("bwa/" + Versions.BWA)),
                new VirusBreakendCommand(resourceFiles, tumorSample, getTumorBamDownload().getLocalTargetPath()));
    }

    @Override
    public VirusBreakendOutput skippedOutput(final SomaticRunMetadata metadata) {
        return VirusBreakendOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.useTargetRegions();
    }

    @Override
    public VirusBreakendOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        String vcf = vcf(metadata);
        String summary = summary(metadata);

        return VirusBreakendOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new SingleFileComponent(bucket, NAMESPACE, Folder.root(), vcf, vcf, resultsDirectory),
                        new SingleFileComponent(bucket, NAMESPACE, Folder.root(), summary, summary, resultsDirectory),
                        new RunLogComponent(bucket, NAMESPACE, Folder.root(), resultsDirectory))
                .maybeSummary(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(summary)))
                .maybeVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(vcf)))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public VirusBreakendOutput persistedOutput(final SomaticRunMetadata metadata) {
        return VirusBreakendOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .addAllDatatypes(addDatatypes(metadata))
                .maybeSummary(persistedDataset.path(metadata.tumor().sampleName(), DataType.VIRUSBREAKEND_SUMMARY)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), summary(metadata)))))
                .maybeVariants(persistedDataset.path(metadata.tumor().sampleName(), DataType.VIRUSBREAKEND_VARIANTS)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), vcf(metadata)))))
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        String vcf = vcf(metadata);
        String summary = summary(metadata);
        return List.of(new AddDatatype(DataType.VIRUSBREAKEND_SUMMARY,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), summary)),
                new AddDatatype(DataType.VIRUSBREAKEND_VARIANTS, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), vcf)));
    }

    private String summary(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + ".virusbreakend.vcf.summary.tsv";
    }

    private String vcf(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + ".virusbreakend.vcf";
    }
}
