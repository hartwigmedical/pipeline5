package com.hartwig.pipeline.tertiary.pave;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
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

import org.jetbrains.annotations.NotNull;

public abstract class Pave implements Stage<PaveOutput, SomaticRunMetadata> {

    public static final String PAVE_FILE_ID = "pave";

    private final ResourceFiles resourceFiles;
    private final InputDownload vcfDownload;
    private final PersistedDataset persistedDataset;
    private final DataType vcfDatatype;

    public Pave(final ResourceFiles resourceFiles, SageOutput sageOutput,
            final PersistedDataset persistedDataset, final DataType vcfDatatype) {
        this.resourceFiles = resourceFiles;
        this.vcfDownload = new InputDownload(sageOutput.finalVcf());
        this.persistedDataset = persistedDataset;
        this.vcfDatatype = vcfDatatype;
    }

    protected abstract String outputFile(final SomaticRunMetadata metadata);

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {

        // build
        PaveCommandBuilder builder = new PaveCommandBuilder(resourceFiles,
                metadata.tumor().sampleName(),
                vcfDownload.getLocalTargetPath());

        return Collections.singletonList(builder.build());
    }

    @Override
    public List<BashCommand> inputs() {
        List<BashCommand> inputs = new ArrayList<>(ImmutableList.of(vcfDownload));

        return inputs;
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.purple(bash, resultsDirectory);
    }

    @Override
    public PaveOutput skippedOutput(final SomaticRunMetadata metadata) {
        return PaveOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @NotNull
    public GoogleStorageLocation persistedOrDefault(final SomaticRunMetadata metadata, final DataType somaticVariantsPave,
            final String s) {
        return persistedDataset.path(metadata.tumor().sampleName(), somaticVariantsPave)
                .orElse(GoogleStorageLocation.of(metadata.bucket(), PersistedLocations.blobForSet(metadata.set(), namespace(), s)));
    }

    @Override
    public PaveOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {

        final String outputFile = outputFile(metadata);

        ImmutablePaveOutput.Builder builder = PaveOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeFinalVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(outputFile)))
                .addReportComponents(vcfComponent(outputFile, bucket, resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.SOMATIC_VARIANTS_PAVE,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), outputFile)));

        return builder.build();
    }

    @Override
    public PaveOutput persistedOutput(final SomaticRunMetadata metadata) {

        // GoogleStorageLocation somaticVariantsLocation = persistedOrDefault(metadata, DataType.SOMATIC_VARIANTS_PAVE, outputFile(metadata));
        final String outputFile = outputFile(metadata);

        return PaveOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeFinalVcf(persistedDataset.path(metadata.tumor().sampleName(), vcfDatatype)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), outputFile))))
                .build();
    }

    private ReportComponent vcfComponent(final String filename, final RuntimeBucket bucket, final ResultsDirectory resultsDirectory) {
        return new ZippedVcfAndIndexComponent(bucket, namespace(), Folder.root(), filename, resultsDirectory);
    }

}