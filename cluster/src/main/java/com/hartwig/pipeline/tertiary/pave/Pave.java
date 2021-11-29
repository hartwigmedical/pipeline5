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
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

import org.jetbrains.annotations.NotNull;

public class Pave implements Stage<PaveOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "pave";
    public static final String PAVE_GERMLINE_VCF = ".pave.germline.vcf.gz";
    public static final String PAVE_SOMATIC_VCF = ".pave.somatic.vcf.gz";

    private final ResourceFiles resourceFiles;
    private final InputDownload somaticVcfDownload;
    private final InputDownload germlineVcfDownload;
    private final PersistedDataset persistedDataset;
    private final boolean sageGermlineEnabled;

    public Pave(final ResourceFiles resourceFiles, SageOutput somaticCallerOutput, SageOutput germlineCallerOutput,
            final PersistedDataset persistedDataset, final boolean sageGermlineEnabled) {
        this.resourceFiles = resourceFiles;
        this.somaticVcfDownload = new InputDownload(somaticCallerOutput.finalVcf());
        this.germlineVcfDownload = new InputDownload(germlineCallerOutput.maybeFinalVcf().orElse(GoogleStorageLocation.empty()));
        this.persistedDataset = persistedDataset;
        this.sageGermlineEnabled = sageGermlineEnabled;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {

        // build
        PaveCommandBuilder builder = new PaveCommandBuilder(resourceFiles,
                metadata.tumor().sampleName(),
                somaticVcfDownload.getLocalTargetPath());

        //if (sageGermlineEnabled) {
        //    builder.addGermline(germlineVcfDownload.getLocalTargetPath());
        // }

        return Collections.singletonList(builder.build());
    }

    @Override
    public List<BashCommand> inputs() {
        List<BashCommand> inputs = new ArrayList<>(ImmutableList.of(somaticVcfDownload));

        if (sageGermlineEnabled) {
            inputs.add(germlineVcfDownload);
        }
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

    private static String somaticVcf(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PAVE_SOMATIC_VCF;
    }

    private static String germlineVcf(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PAVE_GERMLINE_VCF;
    }

    @Override
    public PaveOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {

        ImmutablePaveOutput.Builder builder = PaveOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeOutputLocations(PaveOutputLocations.builder()
                        .outputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                        .germlineVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(germlineVcf(metadata))))
                        .somaticVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticVcf(metadata))))
                        .build())
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.SOMATIC_VARIANTS_PAVE,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), somaticVcf(metadata))));

        if (sageGermlineEnabled) {
            builder.addDatatypes(new AddDatatype(DataType.GERMLINE_VARIANTS_PAVE,
                    metadata.barcode(),
                    new ArchivePath(Folder.root(), namespace(), germlineVcf(metadata))));
        }
        return builder.build();
    }

    @Override
    public PaveOutput persistedOutput(final SomaticRunMetadata metadata) {

        GoogleStorageLocation germlineVariantsLocation =
                persistedOrDefault(metadata, DataType.GERMLINE_VARIANTS_PAVE, germlineVcf(metadata));

        GoogleStorageLocation somaticVariantsLocation =
                persistedOrDefault(metadata, DataType.SOMATIC_VARIANTS_PAVE, somaticVcf(metadata));

        return PaveOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeOutputLocations(PaveOutputLocations.builder()
                        .outputDirectory(somaticVariantsLocation.transform(f -> new File(f).getParent()).asDirectory())
                        .somaticVcf(somaticVariantsLocation)
                        .germlineVcf(germlineVariantsLocation)
                        .build())
                .build();
    }
}