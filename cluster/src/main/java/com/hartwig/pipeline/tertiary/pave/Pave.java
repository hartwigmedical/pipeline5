package com.hartwig.pipeline.tertiary.pave;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.ReportComponent;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public abstract class Pave implements Stage<PaveOutput, SomaticRunMetadata> {

    static final String PAVE_FILE_NAME = "pave";

    protected final ResourceFiles resourceFiles;
    protected final InputDownload vcfDownload;
    private final PersistedDataset persistedDataset;
    private final DataType vcfDatatype;

    public Pave(
            final ResourceFiles resourceFiles, final SageOutput sageOutput, final PersistedDataset persistedDataset, final DataType vcfDatatype) {
        this.resourceFiles = resourceFiles;
        this.vcfDownload = new InputDownload(sageOutput.variants());
        this.persistedDataset = persistedDataset;
        this.vcfDatatype = vcfDatatype;
    }

    protected abstract String outputFile(final SomaticRunMetadata metadata);

    protected List<BashCommand> paveCommand(final SomaticRunMetadata metadata, final List<String> arguments)
    {
        return Collections.singletonList(new JavaJarCommand("pave", Versions.PAVE, "pave.jar", "16G", arguments));
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(vcfDownload);
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {

        return VirtualMachineJobDefinition.pave(namespace().replace("_", "-"), bash, resultsDirectory);
    }

    @Override
    public PaveOutput skippedOutput(final SomaticRunMetadata metadata) {
        return PaveOutput.builder(namespace()).status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public PaveOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        final String outputFile = outputFile(metadata);
        return PaveOutput.builder(namespace())
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeAnnotatedVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(outputFile)))
                .addReportComponents(vcfComponent(outputFile, bucket, resultsDirectory))
                .addReportComponents(new RunLogComponent(bucket, namespace(), Folder.root(), ResultsDirectory.defaultDirectory()))
                .addDatatypes(new AddDatatype(vcfDatatype, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), outputFile)))
                .build();
    }

    @Override
    public PaveOutput persistedOutput(final SomaticRunMetadata metadata) {
        final String outputFile = outputFile(metadata);
        return PaveOutput.builder(namespace())
                .status(PipelineStatus.PERSISTED)
                .maybeAnnotatedVariants(persistedDataset.path(metadata.tumor().sampleName(), vcfDatatype)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), outputFile))))
                .build();
    }

    private ReportComponent vcfComponent(final String filename, final RuntimeBucket bucket, final ResultsDirectory resultsDirectory) {
        return new ZippedVcfAndIndexComponent(bucket, namespace(), Folder.root(), filename, resultsDirectory);
    }
}