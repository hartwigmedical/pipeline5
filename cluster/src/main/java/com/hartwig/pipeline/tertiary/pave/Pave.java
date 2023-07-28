package com.hartwig.pipeline.tertiary.pave;

import static com.hartwig.pipeline.tools.HmfTool.PAVE;

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
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.OutputComponent;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.output.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public abstract class Pave implements Stage<PaveOutput, SomaticRunMetadata> {

    protected final ResourceFiles resourceFiles;
    protected final InputDownload vcfDownload;
    private final PersistedDataset persistedDataset;
    private final DataType vcfDatatype;

    public Pave(final ResourceFiles resourceFiles, final SageOutput sageOutput, final PersistedDataset persistedDataset,
            final DataType vcfDatatype) {
        this.resourceFiles = resourceFiles;
        this.vcfDownload = new InputDownload(sageOutput.variants());
        this.persistedDataset = persistedDataset;
        this.vcfDatatype = vcfDatatype;
    }

    protected abstract String outputFile(final SomaticRunMetadata metadata);

    protected List<BashCommand> paveCommand(final List<String> arguments) {
        return Collections.singletonList(new JavaJarCommand(PAVE, arguments));
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
                .addAllDatatypes(addDatatypes(metadata))
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
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        return List.of(new AddDatatype(vcfDatatype, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), outputFile(metadata))));
    }

    private OutputComponent vcfComponent(final String filename, final RuntimeBucket bucket, final ResultsDirectory resultsDirectory) {
        return new ZippedVcfAndIndexComponent(bucket, namespace(), Folder.root(), filename, resultsDirectory);
    }
}