package com.hartwig.pipeline.tertiary.virus;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;

import org.jetbrains.annotations.NotNull;

public class VirusAnalysis extends TertiaryStage<VirusOutput> {

    public static final String NAMESPACE = "virusbreakend";

    public static final String VIRUS_BREAKEND_SUMMARY = ".virusbreakend.vcf.summary.tsv";
    public static final String ANNOTATED_VIRUS_TSV = ".virus.annotated.tsv";

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public VirusAnalysis(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
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
        String tumorSample = metadata.tumor().sampleName();
        return new VirusBreakend(tumorSample, getTumorBamDownload().getLocalTargetPath(), resourceFiles).andThen(new VirusInterpreter(
                tumorSample,
                resourceFiles)).apply(SubStageInputOutput.empty(tumorSample)).bash();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.virusbreakend(bash, resultsDirectory);
    }

    @Override
    public VirusOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        String vcf = vcf(metadata);
        String summary = summary(metadata);
        String annotated = annotatedVirusTsv(metadata);

        return VirusOutput.builder()
                .status(jobStatus)
                .summaryFile(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(summary)))
                .annotatedVirusFile(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(annotated)))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new SingleFileComponent(bucket, NAMESPACE, Folder.root(), vcf, vcf, resultsDirectory),
                        new SingleFileComponent(bucket, NAMESPACE, Folder.root(), summary, summary, resultsDirectory),
                        new SingleFileComponent(bucket, NAMESPACE, Folder.root(), annotated, annotated, resultsDirectory),
                        new RunLogComponent(bucket, NAMESPACE, Folder.root(), resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.VIRUSBREAKEND_VARIANTS,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), vcf)),
                        new AddDatatype(DataType.VIRUSBREAKEND_SUMMARY,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), summary)),
                        new AddDatatype(DataType.VIRUS_INTERPRETATION,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), annotated)))
                .build();
    }

    @NotNull
    protected String annotatedVirusTsv(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + ANNOTATED_VIRUS_TSV;
    }

    @NotNull
    protected String summary(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + VIRUS_BREAKEND_SUMMARY;
    }

    @NotNull
    protected String vcf(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + ".virusbreakend.vcf";
    }

    @Override
    public VirusOutput skippedOutput(final SomaticRunMetadata metadata) {
        return VirusOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public VirusOutput persistedOutput(final SomaticRunMetadata metadata) {
        String vcf = vcf(metadata);
        String summary = summary(metadata);
        String annotated = annotatedVirusTsv(metadata);
        return VirusOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .summaryFile(persistedDataset.path(metadata.tumor().sampleName(), DataType.VIRUSBREAKEND_SUMMARY)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), summary))))
                .annotatedVirusFile(persistedDataset.path(metadata.tumor().sampleName(), DataType.VIRUS_INTERPRETATION)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), annotated))))
                .addDatatypes(new AddDatatype(DataType.VIRUSBREAKEND_SUMMARY,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), summary)))
                .addDatatypes(new AddDatatype(DataType.VIRUS_INTERPRETATION,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), annotated)))
                .addDatatypes(new AddDatatype(DataType.VIRUSBREAKEND_VARIANTS,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), vcf)))
                .build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary();
    }
}
