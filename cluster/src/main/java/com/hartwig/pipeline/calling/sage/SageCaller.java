package com.hartwig.pipeline.calling.sage;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.EntireOutputComponent;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.OutputComponent;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.output.SingleFileComponent;
import com.hartwig.pipeline.output.StartupScriptComponent;
import com.hartwig.pipeline.output.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;

public abstract class SageCaller extends TertiaryStage<SageOutput> {

    public static final String SAGE_GENE_COVERAGE_TSV = "sage.gene.coverage.tsv";
    public static final String SAGE_BQR_PNG = "sage.bqr.png";

    protected final PersistedDataset persistedDataset;
    protected final SageConfiguration sageConfiguration;

    public SageCaller(final AlignmentPair alignmentPair, final PersistedDataset persistedDataset,
            final SageConfiguration sageConfiguration) {
        super(alignmentPair);
        this.persistedDataset = persistedDataset;
        this.sageConfiguration = sageConfiguration;
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        return new SageApplication(sageConfiguration.commandBuilder()
                .addTumor(metadata.tumor().sampleName(), getTumorBamDownload().getLocalTargetPath())
                .addReference(metadata.reference().sampleName(),
                        getReferenceBamDownload().getLocalTargetPath())).andThen(sageConfiguration.postProcess().apply(metadata))
                .apply(SubStageInputOutput.empty(metadata.tumor().sampleName()))
                .bash();
    }

    @Override
    public String namespace() {
        return sageConfiguration.namespace();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return sageConfiguration.jobDefinition().apply(bash, resultsDirectory);
    }

    @Override
    public SageOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return outputBuilder(metadata, jobStatus, bucket, resultsDirectory).build();
    }

    protected ImmutableSageOutput.Builder outputBuilder(final SomaticRunMetadata metadata, final PipelineStatus jobStatus,
            final RuntimeBucket bucket, final ResultsDirectory resultsDirectory) {

        final String filteredOutputFile = sageConfiguration.filteredTemplate().apply(metadata);
        final String geneCoverageFile = sageConfiguration.geneCoverageTemplate().apply(metadata);
        final Optional<String> somaticRefSampleBqrPlot = referenceSampleBqrPlot(metadata);
        final Optional<String> somaticTumorSampleBqrPlot = tumorSampleBqrPlot(metadata);
        final ImmutableSageOutput.Builder builder = SageOutput.builder(namespace()).status(jobStatus);
        somaticRefSampleBqrPlot.ifPresent(s -> builder.maybeSomaticRefSampleBqrPlot(GoogleStorageLocation.of(bucket.name(),
                        resultsDirectory.path(s)))
                .addReportComponents(bqrComponent("png", bucket, resultsDirectory, metadata.reference().sampleName()))
                .addReportComponents(bqrComponent("tsv", bucket, resultsDirectory, metadata.reference().sampleName())));
        somaticTumorSampleBqrPlot.ifPresent(s -> builder.maybeSomaticTumorSampleBqrPlot(GoogleStorageLocation.of(bucket.name(),
                        resultsDirectory.path(s)))
                .addReportComponents(bqrComponent("png", bucket, resultsDirectory, metadata.tumor().sampleName()))
                .addReportComponents(bqrComponent("tsv", bucket, resultsDirectory, metadata.tumor().sampleName())));
        return builder.addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeGermlineGeneCoverage(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(geneCoverageFile)))
                .maybeSomaticTumorSampleBqrPlot(somaticTumorSampleBqrPlot.map(t -> GoogleStorageLocation.of(bucket.name(),
                        resultsDirectory.path(t))))
                .maybeVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(filteredOutputFile)))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addReportComponents(new RunLogComponent(bucket, namespace(), Folder.root(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, namespace(), Folder.root()))
                .addAllDatatypes(addDatatypes(metadata));
    }

    @Override
    public SageOutput skippedOutput(final SomaticRunMetadata metadata) {
        return SageOutput.builder(namespace()).status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public final SageOutput persistedOutput(final SomaticRunMetadata metadata) {
        final String filteredOutputFile = sageConfiguration.filteredTemplate().apply(metadata);
        final String geneCoverageFile = sageConfiguration.geneCoverageTemplate().apply(metadata);
        final Optional<String> somaticRefSampleBqrPlot = referenceSampleBqrPlot(metadata);
        final Optional<String> somaticTumorSampleBqrPlot = tumorSampleBqrPlot(metadata);
        final ImmutableSageOutput.Builder builder = SageOutput.builder(namespace())
                .status(PipelineStatus.PERSISTED)
                .maybeVariants(persistedDataset.path(metadata.tumor().sampleName(), sageConfiguration.vcfDatatype())
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), filteredOutputFile))))
                .maybeGermlineGeneCoverage(persistedDataset.path(metadata.tumor().sampleName(), sageConfiguration.geneCoverageDatatype())
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), geneCoverageFile))));
        somaticRefSampleBqrPlot.ifPresent(r -> builder.maybeSomaticRefSampleBqrPlot(persistedDataset.path(metadata.tumor().sampleName(),
                        sageConfiguration.refSampleBqrPlot())
                .orElse(GoogleStorageLocation.of(metadata.bucket(), PersistedLocations.blobForSet(metadata.set(), namespace(), r)))));
        somaticTumorSampleBqrPlot.ifPresent(r -> builder.maybeSomaticTumorSampleBqrPlot(persistedDataset.path(metadata.tumor().sampleName(),
                        sageConfiguration.tumorSampleBqrPlot())
                .orElse(GoogleStorageLocation.of(metadata.bucket(), PersistedLocations.blobForSet(metadata.set(), namespace(), r)))));
        return builder.addAllDatatypes(addDatatypes(metadata)).build();
    }

    @Override
    public final List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        final String filteredOutputFile = sageConfiguration.filteredTemplate().apply(metadata);
        final String geneCoverageFile = sageConfiguration.geneCoverageTemplate().apply(metadata);
        final Optional<String> somaticRefSampleBqrPlot = referenceSampleBqrPlot(metadata);
        final Optional<String> somaticTumorSampleBqrPlot = tumorSampleBqrPlot(metadata);

        ArrayList<AddDatatype> datatypes = new ArrayList<>(List.of(new AddDatatype(sageConfiguration.vcfDatatype(),
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), filteredOutputFile)),
                new AddDatatype(sageConfiguration.geneCoverageDatatype(),
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), geneCoverageFile))));
        datatypes.addAll(somaticRefSampleBqrPlot.stream()
                .map(r -> new AddDatatype(sageConfiguration.tumorSampleBqrPlot(),
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), r)))
                .collect(Collectors.toList()));
        datatypes.addAll(somaticTumorSampleBqrPlot.stream()
                .map(t -> new AddDatatype(sageConfiguration.refSampleBqrPlot(),
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), t)))
                .collect(Collectors.toList()));
        return datatypes;
    }

    protected OutputComponent singleFileComponent(final String filename, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return new SingleFileComponent(bucket, namespace(), Folder.root(), filename, filename, resultsDirectory);
    }

    private Optional<String> tumorSampleBqrPlot(final SomaticRunMetadata metadata) {
        return metadata.maybeTumor().map(t -> String.format("%s.%s", t.sampleName(), SAGE_BQR_PNG));
    }

    private Optional<String> referenceSampleBqrPlot(final SomaticRunMetadata metadata) {
        return metadata.maybeReference().map(r -> String.format("%s.%s", r.sampleName(), SAGE_BQR_PNG));
    }

    private OutputComponent bqrComponent(final String extension, final RuntimeBucket bucket, final ResultsDirectory resultsDirectory,
            final String sampleName) {
        String filename = format("%s.sage.bqr.%s", sampleName, extension);
        return singleFileComponent(filename, bucket, resultsDirectory);
    }

    private OutputComponent vcfComponent(final String filename, final RuntimeBucket bucket, final ResultsDirectory resultsDirectory) {
        return new ZippedVcfAndIndexComponent(bucket, namespace(), Folder.root(), filename, resultsDirectory);
    }
}