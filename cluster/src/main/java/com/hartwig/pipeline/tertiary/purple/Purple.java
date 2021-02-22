package com.hartwig.pipeline.tertiary.purple;

import java.io.File;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.calling.structural.StructuralCallerPostProcessOutput;
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
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;

import org.jetbrains.annotations.NotNull;

public class Purple implements Stage<PurpleOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "purple";
    public static final String PURPLE_SOMATIC_VCF = ".purple.somatic.vcf.gz";
    public static final String PURPLE_SV_VCF = ".purple.sv.vcf.gz";
    public static final String PURPLE_PURITY_TSV = ".purple.purity.tsv";
    public static final String PURPLE_QC = ".purple.qc";
    public static final String PURPLE_DRIVER_CATALOG = ".driver.catalog.somatic.tsv";

    private final ResourceFiles resourceFiles;
    private final InputDownload somaticVcfDownload;
    private final InputDownload germlineVcfDownload;
    private final InputDownload structuralVcfDownload;
    private final InputDownload structuralVcfIndexDownload;
    private final InputDownload svRecoveryVcfDownload;
    private final InputDownload svRecoveryVcfIndexDownload;
    private final InputDownload amberOutputDownload;
    private final InputDownload cobaltOutputDownload;
    private final PersistedDataset persistedDataset;
    private final boolean shallow;

    public Purple(final ResourceFiles resourceFiles, SageOutput somaticCallerOutput, SageOutput germlineCallerOutput,
            StructuralCallerPostProcessOutput structuralCallerOutput, AmberOutput amberOutput, CobaltOutput cobaltOutput,
            final PersistedDataset persistedDataset, final boolean shallow) {
        this.resourceFiles = resourceFiles;
        somaticVcfDownload = new InputDownload(somaticCallerOutput.finalVcf());
        germlineVcfDownload = new InputDownload(germlineCallerOutput.finalVcf());
        structuralVcfDownload = new InputDownload(structuralCallerOutput.filteredVcf());
        structuralVcfIndexDownload = new InputDownload(structuralCallerOutput.filteredVcfIndex());
        svRecoveryVcfDownload = new InputDownload(structuralCallerOutput.fullVcf());
        svRecoveryVcfIndexDownload = new InputDownload(structuralCallerOutput.fullVcfIndex());
        amberOutputDownload = new InputDownload(amberOutput.outputDirectory());
        cobaltOutputDownload = new InputDownload(cobaltOutput.outputDirectory());
        this.persistedDataset = persistedDataset;
        this.shallow = shallow;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        BashCommand command = new PurpleCommandBuilder(resourceFiles,
                amberOutputDownload.getLocalTargetPath(),
                cobaltOutputDownload.getLocalTargetPath(),
                metadata.tumor().sampleName(),
                structuralVcfDownload.getLocalTargetPath(),
                svRecoveryVcfDownload.getLocalTargetPath(),
                somaticVcfDownload.getLocalTargetPath()).addGermline(metadata.reference().sampleName(),
                germlineVcfDownload.getLocalTargetPath()).setShallow(shallow).build();

        return Collections.singletonList(command);
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(somaticVcfDownload,
                germlineVcfDownload,
                structuralVcfDownload,
                structuralVcfIndexDownload,
                svRecoveryVcfDownload,
                svRecoveryVcfIndexDownload,
                amberOutputDownload,
                cobaltOutputDownload);
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
    public PurpleOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return PurpleOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeOutputLocations(PurpleOutputLocations.builder()
                        .outputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                        .somaticVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticVcf(metadata))))
                        .structuralVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(svVcf(metadata))))
                        .purityTsv(GoogleStorageLocation.of(bucket.name(),
                                resultsDirectory.path(metadata.tumor().name() + PURPLE_PURITY_TSV)))
                        .driverCatalog(GoogleStorageLocation.of(bucket.name(),
                                resultsDirectory.path(metadata.tumor().name() + PURPLE_DRIVER_CATALOG)))
                        .qcFile(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(metadata.tumor().name() + PURPLE_QC)))
                        .build())
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory))
                .addFurtherOperations(new AddDatatype(DataType.SOMATIC_VARIANTS_PURPLE,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), somaticVcf(metadata))),
                        new AddDatatype(DataType.STRUCTURAL_VARIANTS_PURPLE,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), svVcf(metadata))))
                .build();
    }

    @Override
    public PurpleOutput skippedOutput(final SomaticRunMetadata metadata) {
        return PurpleOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public PurpleOutput persistedOutput(final SomaticRunMetadata metadata) {
        GoogleStorageLocation somaticVariantsLocation =
                persistedOrDefault(metadata, DataType.SOMATIC_VARIANTS_PURPLE, somaticVcf(metadata));
        GoogleStorageLocation svsLocation = persistedOrDefault(metadata, DataType.STRUCTURAL_VARIANTS_PURPLE, svVcf(metadata));
        GoogleStorageLocation purityLocation =
                persistedOrDefault(metadata, DataType.PURPLE_PURITY, metadata.tumor().name() + PURPLE_PURITY_TSV);
        GoogleStorageLocation driverCatalogLocation =
                persistedOrDefault(metadata, DataType.PURPLE_DRIVER_CATALOG, metadata.tumor().name() + PURPLE_DRIVER_CATALOG);
        GoogleStorageLocation qcLocation = persistedOrDefault(metadata, DataType.PURPLE_QC, metadata.tumor().name() + PURPLE_QC);
        return PurpleOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeOutputLocations(PurpleOutputLocations.builder()
                        .outputDirectory(somaticVariantsLocation.transform(f -> new File(f).getParent()).asDirectory())
                        .somaticVcf(somaticVariantsLocation)
                        .structuralVcf(svsLocation)
                        .purityTsv(purityLocation)
                        .driverCatalog(driverCatalogLocation)
                        .qcFile(qcLocation)
                        .build())
                .build();
    }

    @NotNull
    public GoogleStorageLocation persistedOrDefault(final SomaticRunMetadata metadata, final DataType somaticVariantsPurple,
            final String s) {
        return persistedDataset.path(metadata.tumor().sampleName(), somaticVariantsPurple)
                .orElse(GoogleStorageLocation.of(metadata.bucket(), PersistedLocations.blobForSet(metadata.set(), namespace(), s)));
    }

    private static String svVcf(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PURPLE_SV_VCF;
    }

    private static String somaticVcf(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PURPLE_SOMATIC_VCF;
    }
}
