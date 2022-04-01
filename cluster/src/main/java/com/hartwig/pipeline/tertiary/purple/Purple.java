package com.hartwig.pipeline.tertiary.purple;

import static java.lang.String.format;

import java.io.File;
import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.structural.gripss.GripssOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
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
import com.hartwig.pipeline.tertiary.pave.PaveOutput;

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
    private final InputDownload somaticVariantsDownload;
    private final InputDownload germlineVariantsDownload;
    private final InputDownload structuralVariantsDownload;
    private final InputDownload structuralVariantsIndexDownload;
    private final InputDownload svRecoveryVariantsDownload;
    private final InputDownload svRecoveryVariantsIndexDownload;
    private final InputDownload amberOutputDownload;
    private final InputDownload cobaltOutputDownload;
    private final PersistedDataset persistedDataset;
    private final boolean shallow;

    public Purple(final ResourceFiles resourceFiles, final PaveOutput paveSomaticOutput, final PaveOutput germlineCallerOutput,
            final GripssOutput gripssOutput, final AmberOutput amberOutput, final CobaltOutput cobaltOutput,
            final PersistedDataset persistedDataset, final boolean shallow) {
        this.resourceFiles = resourceFiles;
        this.somaticVariantsDownload = new InputDownload(paveSomaticOutput.annotatedVariants());
        this.germlineVariantsDownload = new InputDownload(germlineCallerOutput.annotatedVariants());
        this.structuralVariantsDownload = new InputDownload(gripssOutput.filteredVariants());
        this.structuralVariantsIndexDownload = new InputDownload(gripssOutput.filteredVariants().transform(FileTypes::tabixIndex));
        this.svRecoveryVariantsDownload = new InputDownload(gripssOutput.unfilteredVariants());
        this.svRecoveryVariantsIndexDownload = new InputDownload(gripssOutput.unfilteredVariants().transform(FileTypes::tabixIndex));
        this.amberOutputDownload = new InputDownload(amberOutput.outputDirectory());
        this.cobaltOutputDownload = new InputDownload(cobaltOutput.outputDirectory());
        this.persistedDataset = persistedDataset;
        this.shallow = shallow;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        return Collections.singletonList(commonCommands(metadata.tumor().sampleName()).build());
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        return Collections.singletonList(commonCommands(metadata.reference()
                .sampleName()).addGermline(germlineVariantsDownload.getLocalTargetPath()).build());
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        return Collections.singletonList(commonCommands(metadata.reference().sampleName()).setReferenceSample(metadata.reference()
                .sampleName()).addGermline(germlineVariantsDownload.getLocalTargetPath()).setShallow(shallow).build());
    }

    private PurpleCommandBuilder commonCommands(final String sample) {
        return new PurpleCommandBuilder(resourceFiles,
                amberOutputDownload.getLocalTargetPath(),
                cobaltOutputDownload.getLocalTargetPath(),
                sample,
                structuralVariantsDownload.getLocalTargetPath(),
                svRecoveryVariantsDownload.getLocalTargetPath(),
                somaticVariantsDownload.getLocalTargetPath());
    }

    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return Collections.singletonList(new PurpleCommandBuilder(resourceFiles,
                amberOutputDownload.getLocalTargetPath(),
                cobaltOutputDownload.getLocalTargetPath(),
                metadata.tumor().sampleName(),
                structuralVariantsDownload.getLocalTargetPath(),
                svRecoveryVariantsDownload.getLocalTargetPath(),
                somaticVariantsDownload.getLocalTargetPath()).setShallow(shallow)
                .setReferenceSample(metadata.reference().sampleName())
                .addGermline(germlineVariantsDownload.getLocalTargetPath())
                .build());
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(somaticVariantsDownload,
                structuralVariantsDownload,
                structuralVariantsIndexDownload,
                svRecoveryVariantsDownload,
                svRecoveryVariantsIndexDownload,
                amberOutputDownload,
                cobaltOutputDownload,
                germlineVariantsDownload);
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
                        .germlineVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(germlineVcf(metadata))))
                        .somaticVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticVcf(metadata))))
                        .structuralVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(svVcf(metadata))))
                        .purity(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(purityTsv)))
                        .qcFile(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(qcFile)))
                        .geneCopyNumber(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(geneCopyNumberTsv(metadata))))
                        .somaticCopyNumber(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticCopyNumberTsv(metadata))))
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
        builder.addDatatypes(new AddDatatype(DataType.GERMLINE_VARIANTS_PURPLE,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), germlineVcf(metadata))))
                .addDatatypes(new AddDatatype(DataType.PURPLE_GERMLINE_DRIVER_CATALOG,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), germlineDriverCatalog)))
                .addDatatypes(new AddDatatype(DataType.PURPLE_GERMLINE_COPY_NUMBER,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), germlineCnv)));
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
                        .somaticVariants(somaticVariantsLocation)
                        .germlineVariants(germlineVariantsLocation)
                        .structuralVariants(svsLocation)
                        .purity(purityLocation)
                        .qcFile(qcLocation)
                        .geneCopyNumber(geneCopyNumberLocation)
                        .somaticDriverCatalog(somaticDriverCatalogLocation)
                        .germlineDriverCatalog(germlineDriverCatalogLocation)
                        .circosPlot(circosPlot)
                        .somaticCopyNumber(somaticCopyNumberLocation)
                        .build())
                .build();
    }
}