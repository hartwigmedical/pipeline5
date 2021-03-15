package com.hartwig.pipeline.calling.sage;

import static java.lang.String.format;

import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.ReportComponent;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.report.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;

public abstract class SageCaller extends TertiaryStage<SageOutput> {

    private final PersistedDataset persistedDataset;
    private final DataType dataType;

    public SageCaller(final AlignmentPair alignmentPair, final PersistedDataset persistedDataset, final DataType dataType) {
        super(alignmentPair);
        this.persistedDataset = persistedDataset;
        this.dataType = dataType;
    }

    protected abstract String filteredOutput(final SomaticRunMetadata metadata);

    protected abstract String unfilteredOutput(final SomaticRunMetadata metadata);

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.sageSomaticCalling(bash, resultsDirectory);
    }

    @Override
    public SageOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return outputBuilder(metadata, jobStatus, bucket, resultsDirectory).build();
    }

    protected ImmutableSageOutput.Builder outputBuilder(final SomaticRunMetadata metadata, final PipelineStatus jobStatus,
            final RuntimeBucket bucket, final ResultsDirectory resultsDirectory) {

        final String filteredOutputFile = filteredOutput(metadata);
        final String unfilteredOutputFile = unfilteredOutput(metadata);

        return SageOutput.builder(namespace())
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeFinalVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(filteredOutputFile)))
                .addReportComponents(bqrComponent(metadata.tumor(), "png", bucket, resultsDirectory))
                .addReportComponents(bqrComponent(metadata.tumor(), "tsv", bucket, resultsDirectory))
                .addReportComponents(bqrComponent(metadata.reference(), "png", bucket, resultsDirectory))
                .addReportComponents(bqrComponent(metadata.reference(), "tsv", bucket, resultsDirectory))
                .addReportComponents(vcfComponent(unfilteredOutputFile, bucket, resultsDirectory))
                .addReportComponents(vcfComponent(filteredOutputFile, bucket, resultsDirectory))
                .addReportComponents(new RunLogComponent(bucket, namespace(), Folder.root(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, namespace(), Folder.root()))
                .addDatatypes(new AddDatatype(dataType,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), filteredOutputFile)));
    }

    @Override
    public SageOutput skippedOutput(final SomaticRunMetadata metadata) {
        return SageOutput.builder(namespace()).status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public final SageOutput persistedOutput(final SomaticRunMetadata metadata) {
        final String filteredOutputFile = filteredOutput(metadata);

        return SageOutput.builder(namespace())
                .status(PipelineStatus.PERSISTED)
                .maybeFinalVcf(persistedDataset.path(metadata.tumor().sampleName(), dataType)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), filteredOutputFile))))
                .build();
    }

    protected ReportComponent singleFileComponent(String filename, final RuntimeBucket bucket, final ResultsDirectory resultsDirectory) {
        return new SingleFileComponent(bucket, namespace(), Folder.root(), filename, filename, resultsDirectory);
    }

    private ReportComponent bqrComponent(final SingleSampleRunMetadata metadata, final String extension, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        String filename = format("%s.sage.bqr.%s", metadata.sampleName(), extension);
        return singleFileComponent(filename, bucket, resultsDirectory);
    }

    private ReportComponent vcfComponent(final String filename, final RuntimeBucket bucket, final ResultsDirectory resultsDirectory) {
        return new ZippedVcfAndIndexComponent(bucket, namespace(), Folder.root(), filename, resultsDirectory);
    }
}