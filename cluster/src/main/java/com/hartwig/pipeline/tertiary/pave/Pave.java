package com.hartwig.pipeline.tertiary.pave;

import static com.hartwig.pipeline.tools.HmfTool.PAVE;

import java.util.Collections;
import java.util.List;

import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.JavaCommandFactory;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
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

public abstract class Pave implements Stage<PaveOutput, SomaticRunMetadata> {

    protected final ResourceFiles resourceFiles;
    protected final InputDownloadCommand vcfDownload;
    protected final InputDownloadCommand vcfIndexDownload;
    private final PersistedDataset persistedDataset;
    private final DataType vcfDatatype;

    public Pave(final ResourceFiles resourceFiles, final SageOutput sageOutput, final PersistedDataset persistedDataset,
            final DataType vcfDatatype) {
        this.resourceFiles = resourceFiles;
        this.vcfDownload = new InputDownloadCommand(sageOutput.variants());
        this.vcfIndexDownload = new InputDownloadCommand(sageOutput.variants().transform(FileTypes::tabixIndex));
        this.persistedDataset = persistedDataset;
        this.vcfDatatype = vcfDatatype;
    }

    protected abstract String outputFile(final SomaticRunMetadata metadata);

    protected List<BashCommand> paveCommand(final List<String> arguments) {
        return Collections.singletonList(JavaCommandFactory.javaJarCommand(PAVE, arguments));
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(vcfDownload, vcfIndexDownload);
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.pave(bash, resultsDirectory, namespace());
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