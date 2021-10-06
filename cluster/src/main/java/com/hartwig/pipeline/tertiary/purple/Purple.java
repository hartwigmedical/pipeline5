package com.hartwig.pipeline.tertiary.purple;

import static java.lang.String.format;

import java.io.File;
import java.util.ArrayList;
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
    public static final String PURPLE_GERMLINE_VCF = ".purple.germline.vcf.gz";
    public static final String PURPLE_SOMATIC_VCF = ".purple.somatic.vcf.gz";
    public static final String PURPLE_SV_VCF = ".purple.sv.vcf.gz";
    public static final String PURPLE_PURITY_TSV = ".purple.purity.tsv";
    public static final String PURPLE_QC = ".purple.qc";
    public static final String PURPLE_SOMATIC_DRIVER_CATALOG = ".driver.catalog.somatic.tsv";
    public static final String PURPLE_GERMLINE_DRIVER_CATALOG = ".driver.catalog.germline.tsv";
    public static final String PURPLE_GERMLINE_COPY_NUMBER_TSV = ".purple.cnv.germline.tsv";
    public static final String PURPLE_GENE_COPY_NUMBER_TSV = ".purple.cnv.gene.tsv";
    public static final String PURPLE_SOMATIC_COPY_NUMBER_TSV = ".purple.cnv.somatic.tsv";
    public static final String PURPLE_CIRCOS_PLOT = ".circos.png";

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
    private final boolean sageGermlineEnabled;

    public Purple(final ResourceFiles resourceFiles, SageOutput somaticCallerOutput, SageOutput germlineCallerOutput,
            StructuralCallerPostProcessOutput structuralCallerOutput, AmberOutput amberOutput, CobaltOutput cobaltOutput,
            final PersistedDataset persistedDataset, final boolean shallow, final boolean sageGermlineEnabled) {
        this.resourceFiles = resourceFiles;
        this.somaticVcfDownload = new InputDownload(somaticCallerOutput.finalVcf());
        this.germlineVcfDownload = new InputDownload(germlineCallerOutput.maybeFinalVcf().orElse(GoogleStorageLocation.empty()));
        this.structuralVcfDownload = new InputDownload(structuralCallerOutput.filteredVcf());
        this.structuralVcfIndexDownload = new InputDownload(structuralCallerOutput.filteredVcfIndex());
        this.svRecoveryVcfDownload = new InputDownload(structuralCallerOutput.fullVcf());
        this.svRecoveryVcfIndexDownload = new InputDownload(structuralCallerOutput.fullVcfIndex());
        this.amberOutputDownload = new InputDownload(amberOutput.outputDirectory());
        this.cobaltOutputDownload = new InputDownload(cobaltOutput.outputDirectory());
        this.persistedDataset = persistedDataset;
        this.shallow = shallow;
        this.sageGermlineEnabled = sageGermlineEnabled;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        PurpleCommandBuilder builder = new PurpleCommandBuilder(resourceFiles,
                amberOutputDownload.getLocalTargetPath(),
                cobaltOutputDownload.getLocalTargetPath(),
                metadata.tumor().sampleName(),
                structuralVcfDownload.getLocalTargetPath(),
                svRecoveryVcfDownload.getLocalTargetPath(),
                somaticVcfDownload.getLocalTargetPath()).setShallow(shallow).setReferenceSample(metadata.reference().sampleName());
        if (sageGermlineEnabled) {
            builder.addGermline(germlineVcfDownload.getLocalTargetPath());
        }
        return Collections.singletonList(builder.build());
    }

    @Override
    public List<BashCommand> inputs() {
        List<BashCommand> inputs = new ArrayList<>(ImmutableList.of(somaticVcfDownload,
                structuralVcfDownload,
                structuralVcfIndexDownload,
                svRecoveryVcfDownload,
                svRecoveryVcfIndexDownload,
                amberOutputDownload,
                cobaltOutputDownload));
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

    private static String somaticCopyNumberTsv(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PURPLE_SOMATIC_COPY_NUMBER_TSV;
    }

    @Override
    public PurpleOutput skippedOutput(final SomaticRunMetadata metadata) {
        return PurpleOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    private static String circosPlot(final SomaticRunMetadata metadata) {
        return format("plot/%s%s", metadata.tumor().sampleName(), PURPLE_CIRCOS_PLOT);
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

    private static String germlineVcf(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PURPLE_GERMLINE_VCF;
    }

    private static String somaticDriverCatalog(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PURPLE_SOMATIC_DRIVER_CATALOG;
    }

    private static String germlineDriverCatalog(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PURPLE_GERMLINE_DRIVER_CATALOG;
    }

    private static String geneCopyNumberTsv(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PURPLE_GENE_COPY_NUMBER_TSV;
    }

    @Override
    public PurpleOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        String purityTsv = purityTsv(metadata);
        String somaticDriverCatalog = somaticDriverCatalog(metadata);
        String germlineDriverCatalog = germlineDriverCatalog(metadata);
        String germlineCnv = germlineCopyNumberTsv(metadata);
        String qcFile = purpleQC(metadata);
        ImmutablePurpleOutput.Builder builder = PurpleOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeOutputLocations(PurpleOutputLocations.builder()
                        .outputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                        .germlineVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(germlineVcf(metadata))))
                        .somaticVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticVcf(metadata))))
                        .structuralVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(svVcf(metadata))))
                        .purityTsv(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(purityTsv)))
                        .qcFile(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(qcFile)))
                        .geneCopyNumberTsv(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(geneCopyNumberTsv(metadata))))
                        .somaticCopyNumberTsv(GoogleStorageLocation.of(bucket.name(),
                                resultsDirectory.path(somaticCopyNumberTsv(metadata))))
                        .somaticDriverCatalog(GoogleStorageLocation.of(bucket.name(),
                                resultsDirectory.path(somaticDriverCatalog(metadata))))
                        .germlineDriverCatalog(GoogleStorageLocation.of(bucket.name(),
                                resultsDirectory.path(germlineDriverCatalog(metadata))))
                        .circosPlot(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(circosPlot(metadata))))
                        .build())
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.SOMATIC_VARIANTS_PURPLE,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), somaticVcf(metadata))))
                .addDatatypes(new AddDatatype(DataType.STRUCTURAL_VARIANTS_PURPLE,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), svVcf(metadata))))
                .addDatatypes(new AddDatatype(DataType.PURPLE_PURITY,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), purityTsv)))
                .addDatatypes(new AddDatatype(DataType.PURPLE_SOMATIC_DRIVER_CATALOG,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), somaticDriverCatalog)))
                .addDatatypes(new AddDatatype(DataType.PURPLE_SOMATIC_COPY_NUMBER,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), somaticCopyNumberTsv(metadata))))
                .addDatatypes(new AddDatatype(DataType.PURPLE_CIRCOS_PLOT,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), circosPlot(metadata))))
                .addDatatypes(new AddDatatype(DataType.PURPLE_QC, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), qcFile)));
        if (sageGermlineEnabled) {
            builder.addDatatypes(new AddDatatype(DataType.GERMLINE_VARIANTS_PURPLE,
                    metadata.barcode(),
                    new ArchivePath(Folder.root(), namespace(), germlineVcf(metadata))))
                    .addDatatypes(new AddDatatype(DataType.PURPLE_GERMLINE_DRIVER_CATALOG,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), germlineDriverCatalog)))
                    .addDatatypes(new AddDatatype(DataType.PURPLE_GERMLINE_COPY_NUMBER,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), germlineCnv)));
        }
        return builder.build();
    }

    private static String purpleQC(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PURPLE_QC;
    }

    private static String germlineCopyNumberTsv(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PURPLE_GERMLINE_COPY_NUMBER_TSV;
    }

    private static String purityTsv(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PURPLE_PURITY_TSV;
    }

    @Override
    public PurpleOutput persistedOutput(final SomaticRunMetadata metadata) {
        GoogleStorageLocation germlineVariantsLocation =
                persistedOrDefault(metadata, DataType.GERMLINE_VARIANTS_PURPLE, germlineVcf(metadata));
        GoogleStorageLocation somaticVariantsLocation =
                persistedOrDefault(metadata, DataType.SOMATIC_VARIANTS_PURPLE, somaticVcf(metadata));
        GoogleStorageLocation svsLocation = persistedOrDefault(metadata, DataType.STRUCTURAL_VARIANTS_PURPLE, svVcf(metadata));
        GoogleStorageLocation purityLocation = persistedOrDefault(metadata, DataType.PURPLE_PURITY, purityTsv(metadata));
        GoogleStorageLocation qcLocation = persistedOrDefault(metadata, DataType.PURPLE_QC, purpleQC(metadata));
        GoogleStorageLocation geneCopyNumberLocation =
                persistedOrDefault(metadata, DataType.PURPLE_GENE_COPY_NUMBER, geneCopyNumberTsv(metadata));
        GoogleStorageLocation somaticDriverCatalogLocation =
                persistedOrDefault(metadata, DataType.PURPLE_SOMATIC_DRIVER_CATALOG, somaticDriverCatalog(metadata));
        GoogleStorageLocation germlineDriverCatalogLocation =
                persistedOrDefault(metadata, DataType.PURPLE_GERMLINE_DRIVER_CATALOG, germlineDriverCatalog(metadata));
        GoogleStorageLocation somaticCopyNumberLocation =
                persistedOrDefault(metadata, DataType.PURPLE_SOMATIC_COPY_NUMBER, somaticCopyNumberTsv(metadata));
        GoogleStorageLocation circosPlot = persistedOrDefault(metadata, DataType.PURPLE_CIRCOS_PLOT, circosPlot(metadata));
        return PurpleOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeOutputLocations(PurpleOutputLocations.builder()
                        .outputDirectory(somaticVariantsLocation.transform(f -> new File(f).getParent()).asDirectory())
                        .somaticVcf(somaticVariantsLocation)
                        .germlineVcf(germlineVariantsLocation)
                        .structuralVcf(svsLocation)
                        .purityTsv(purityLocation)
                        .qcFile(qcLocation)
                        .geneCopyNumberTsv(geneCopyNumberLocation)
                        .somaticDriverCatalog(somaticDriverCatalogLocation)
                        .germlineDriverCatalog(germlineDriverCatalogLocation)
                        .circosPlot(circosPlot)
                        .somaticCopyNumberTsv(somaticCopyNumberLocation)
                        .build())
                .build();
    }
}