package com.hartwig.pipeline.tertiary.purple;

import static java.lang.String.format;

import static com.hartwig.pipeline.metadata.InputMode.REFERENCE_ONLY;
import static com.hartwig.pipeline.metadata.InputMode.TUMOR_ONLY;
import static com.hartwig.pipeline.metadata.InputMode.TUMOR_REFERENCE;

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
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.InputMode;
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
import com.hartwig.pipeline.tertiary.pave.PaveArgumentBuilder;
import com.hartwig.pipeline.tertiary.pave.PaveOutput;
import com.hartwig.pipeline.tools.Versions;

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
    public static final String PURPLE_GERMLINE_DELETION_TSV = ".purple.germline.deletion.tsv";
    public static final String PURPLE_GENE_COPY_NUMBER_TSV = ".purple.cnv.gene.tsv";
    public static final String PURPLE_SOMATIC_COPY_NUMBER_TSV = ".purple.cnv.somatic.tsv";
    public static final String PURPLE_CIRCOS_PLOT = ".circos.png";

    protected final ResourceFiles resourceFiles;
    protected final InputDownload somaticVariantsDownload;
    protected final InputDownload germlineVariantsDownload;
    protected final InputDownload structuralVariantsDownload;
    protected final InputDownload structuralVariantsIndexDownload;
    protected final InputDownload svRecoveryVariantsDownload;
    protected final InputDownload svRecoveryVariantsIndexDownload;
    protected final InputDownload amberOutputDownload;
    protected final InputDownload cobaltOutputDownload;
    protected final PersistedDataset persistedDataset;
    protected final boolean shallow;

    public Purple(final ResourceFiles resourceFiles, final PaveOutput paveSomaticOutput, final PaveOutput paveGermlineOutput,
            final GripssOutput gripssOutput, final AmberOutput amberOutput, final CobaltOutput cobaltOutput,
            final PersistedDataset persistedDataset, final boolean shallow) {
        this.resourceFiles = resourceFiles;
        this.somaticVariantsDownload = new InputDownload(paveSomaticOutput.annotatedVariants());
        this.germlineVariantsDownload = new InputDownload(paveGermlineOutput.annotatedVariants());
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
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        return buildCommand(metadata);
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata)
    {
        return buildCommand(metadata);
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        return buildCommand(metadata);
    }

    private List<BashCommand> buildCommand(final SomaticRunMetadata metadata) {

        List<String> arguments = PurpleArgumentBuilder.buildArguments(this, metadata, resourceFiles);

        if(shallow)
            PurpleArgumentBuilder.addShallowArguments(arguments);

        return Collections.singletonList(new JavaJarCommand(
                "purple", Versions.PURPLE, "purple.jar", "12G", arguments));
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

        String runId = runSampleId(metadata);
        return persistedDataset.path(runId, somaticVariantsPurple)
                .orElse(GoogleStorageLocation.of(metadata.bucket(), PersistedLocations.blobForSet(metadata.set(), namespace(), s)));
    }

    // common
    private static String purpleQC(final SomaticRunMetadata metadata) {
        return runSampleId(metadata) + PURPLE_QC;
    }

    private static String purityTsv(final SomaticRunMetadata metadata) {
        return runSampleId(metadata) + PURPLE_PURITY_TSV;
    }

    // somatic
    private static String somaticCopyNumberTsv(final SomaticRunMetadata metadata) {
        return metadata.mode().runTumor() ? metadata.tumor().sampleName() + PURPLE_SOMATIC_COPY_NUMBER_TSV : null;
    }

    private static String svVcf(final SomaticRunMetadata metadata) {
        return metadata.mode().runTumor() ? metadata.tumor().sampleName() + PURPLE_SV_VCF : null;
    }

    private static String somaticVcf(final SomaticRunMetadata metadata) {
        return metadata.mode().runTumor() ? metadata.tumor().sampleName() + PURPLE_SOMATIC_VCF : null;
    }

    private static String somaticDriverCatalog(final SomaticRunMetadata metadata) {
        return metadata.mode().runTumor() ? metadata.tumor().sampleName() + PURPLE_SOMATIC_DRIVER_CATALOG : null;
    }

    private static String geneCopyNumberTsv(final SomaticRunMetadata metadata) {
        return metadata.mode().runTumor() ? metadata.tumor().sampleName() + PURPLE_GENE_COPY_NUMBER_TSV : null;
    }

    // germline
    private static String germlineVcf(final SomaticRunMetadata metadata) {
        return runSampleId(metadata) + PURPLE_GERMLINE_VCF;
    }

    private static String germlineDriverCatalog(final SomaticRunMetadata metadata) {
        return runSampleId(metadata) + PURPLE_GERMLINE_DRIVER_CATALOG;
    }

    private static String germlineDeletionTsv(final SomaticRunMetadata metadata) {
        return runSampleId(metadata) + PURPLE_GERMLINE_DELETION_TSV;
    }

    private static String runSampleId(final SomaticRunMetadata metadata) {
        return metadata.mode() == REFERENCE_ONLY ? metadata.reference().sampleName() : metadata.tumor().sampleName();
    }

    @Override
    public PurpleOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {

        String purityTsv = purityTsv(metadata);
        String germlineDriverCatalog = germlineDriverCatalog(metadata);
        String germlineDeletionTsv = germlineDeletionTsv(metadata);
        String qcFile = purpleQC(metadata);
        String germlineVcf = germlineVcf(metadata);

        ImmutablePurpleOutputLocations.Builder outputLocationsBuilder = PurpleOutputLocations.builder()
                .outputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                .purity(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(purityTsv)))
                .qcFile(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(qcFile)));

        ImmutablePurpleOutput.Builder outputBuilder = PurpleOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.PURPLE_PURITY, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), purityTsv)))
                .addDatatypes(new AddDatatype(DataType.PURPLE_QC, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), qcFile)));

        if(metadata.mode().runTumor())
        {
            String somaticDriverCatalog = somaticDriverCatalog(metadata);
            String somaticVcf = somaticVcf(metadata);
            String svVcf = svVcf(metadata);
            String geneCopyNumberTsv = geneCopyNumberTsv(metadata);
            String somaticCopyNumberTsv = somaticCopyNumberTsv(metadata);

            outputLocationsBuilder
                    .somaticVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticVcf)))
                    .structuralVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(svVcf)))
                    .geneCopyNumber(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(geneCopyNumberTsv)))
                    .somaticCopyNumber(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticCopyNumberTsv)))
                    .somaticDriverCatalog(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticDriverCatalog)))
                    .circosPlot(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(circosPlot(metadata))));

            outputBuilder
                    .addDatatypes(new AddDatatype(DataType.SOMATIC_VARIANTS_PURPLE,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), somaticVcf)))
                    .addDatatypes(new AddDatatype(DataType.STRUCTURAL_VARIANTS_PURPLE,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), svVcf)))
                    .addDatatypes(new AddDatatype(DataType.PURPLE_SOMATIC_DRIVER_CATALOG,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), somaticDriverCatalog)))
                    .addDatatypes(new AddDatatype(DataType.PURPLE_SOMATIC_COPY_NUMBER,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), somaticCopyNumberTsv)))
                    .addDatatypes(new AddDatatype(DataType.PURPLE_CIRCOS_PLOT,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), circosPlot(metadata))));
        }

        if(metadata.mode().runGermline())
        {
            outputLocationsBuilder
                    .germlineVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(germlineVcf)))
                    .germlineDriverCatalog(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(germlineDriverCatalog(metadata))))
                    .germlineDeletions(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(germlineDeletionTsv)));

            /*
            outputLocationsBuilder
                    .somaticVariants(null)
                    .structuralVariants(null)
                    .geneCopyNumber(null)
                    .somaticCopyNumber(null)
                    .somaticDriverCatalog(null)
                    .circosPlot(null);
             */

            outputBuilder
                    .addDatatypes(new AddDatatype(
                            DataType.GERMLINE_VARIANTS_PURPLE, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), germlineVcf)))
                    .addDatatypes(new AddDatatype(
                            DataType.PURPLE_GERMLINE_DRIVER_CATALOG, metadata.barcode(), new ArchivePath(Folder.root(),
                            namespace(), germlineDriverCatalog)))
                    .addDatatypes(new AddDatatype(
                            DataType.PURPLE_GERMLINE_DELETION, metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), germlineDeletionTsv)));
        }

        outputBuilder.maybeOutputLocations(outputLocationsBuilder.build());

        /*
        ImmutablePurpleOutput.Builder builder = PurpleOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeOutputLocations(outputLocationsBuilder.build())
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.SOMATIC_VARIANTS_PURPLE,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), somaticVcf)))
                .addDatatypes(new AddDatatype(DataType.STRUCTURAL_VARIANTS_PURPLE,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), svVcf)))
                .addDatatypes(new AddDatatype(DataType.PURPLE_PURITY,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), purityTsv)))
                .addDatatypes(new AddDatatype(DataType.PURPLE_SOMATIC_DRIVER_CATALOG,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), somaticDriverCatalog)))
                .addDatatypes(new AddDatatype(DataType.PURPLE_SOMATIC_COPY_NUMBER,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), somaticCopyNumberTsv)))
                .addDatatypes(new AddDatatype(DataType.PURPLE_CIRCOS_PLOT,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), circosPlot(metadata))))
                .addDatatypes(new AddDatatype(DataType.PURPLE_QC, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), qcFile)));
         */

        return outputBuilder.build();
    }

    @Override
    public PurpleOutput persistedOutput(final SomaticRunMetadata metadata) {

        String purityTsv = purityTsv(metadata);
        String qcFile = purpleQC(metadata);

        GoogleStorageLocation purityLocation = persistedOrDefault(metadata, DataType.PURPLE_PURITY, purityTsv);
        GoogleStorageLocation qcLocation = persistedOrDefault(metadata, DataType.PURPLE_QC, qcFile);

        ImmutablePurpleOutputLocations.Builder outputLocationsBuilder = PurpleOutputLocations.builder()
                .outputDirectory(purityLocation.transform(f -> new File(f).getParent()).asDirectory())
                .purity(purityLocation)
                .qcFile(qcLocation);

        if(metadata.mode().runTumor())
        {
            String somaticDriverCatalog = somaticDriverCatalog(metadata);
            String somaticVcf = somaticVcf(metadata);
            String svVcf = svVcf(metadata);
            String geneCopyNumberTsv = geneCopyNumberTsv(metadata);
            String somaticCopyNumberTsv = somaticCopyNumberTsv(metadata);

            GoogleStorageLocation somaticVariantsLocation =
                    persistedOrDefault(metadata, DataType.SOMATIC_VARIANTS_PURPLE, somaticVcf);
            GoogleStorageLocation svsLocation = persistedOrDefault(metadata, DataType.STRUCTURAL_VARIANTS_PURPLE, svVcf);
            GoogleStorageLocation geneCopyNumberLocation =
                    persistedOrDefault(metadata, DataType.PURPLE_GENE_COPY_NUMBER, geneCopyNumberTsv);
            GoogleStorageLocation somaticDriverCatalogLocation =
                    persistedOrDefault(metadata, DataType.PURPLE_SOMATIC_DRIVER_CATALOG, somaticDriverCatalog);
            GoogleStorageLocation somaticCopyNumberLocation =
                    persistedOrDefault(metadata, DataType.PURPLE_SOMATIC_COPY_NUMBER, somaticCopyNumberTsv);
            GoogleStorageLocation circosPlot = persistedOrDefault(metadata, DataType.PURPLE_CIRCOS_PLOT, circosPlot(metadata));
            outputLocationsBuilder
                    .somaticVariants(somaticVariantsLocation)
                    .structuralVariants(svsLocation)
                    .purity(purityLocation)
                    .qcFile(qcLocation)
                    .geneCopyNumber(geneCopyNumberLocation)
                    .somaticDriverCatalog(somaticDriverCatalogLocation)
                    .circosPlot(circosPlot)
                    .somaticCopyNumber(somaticCopyNumberLocation);
        }

        if(metadata.mode().runGermline())
        {
            String germlineDriverCatalog = germlineDriverCatalog(metadata);
            String germlineDeletionTsv = germlineDeletionTsv(metadata);
            String germlineVcf = germlineVcf(metadata);

            GoogleStorageLocation germlineVariantsLocation =
                    persistedOrDefault(metadata, DataType.GERMLINE_VARIANTS_PURPLE, germlineVcf);
            GoogleStorageLocation germlineDriverCatalogLocation =
                    persistedOrDefault(metadata, DataType.PURPLE_GERMLINE_DRIVER_CATALOG, germlineDriverCatalog);
            GoogleStorageLocation germlineDeletionLocation =
                    persistedOrDefault(metadata, DataType.PURPLE_GERMLINE_DELETION, germlineDeletionTsv);

            outputLocationsBuilder
                    .germlineVariants(germlineVariantsLocation)
                    .germlineDriverCatalog(germlineDriverCatalogLocation)
                    .germlineDeletions(germlineDeletionLocation);
        }

        return PurpleOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeOutputLocations(outputLocationsBuilder.build())
                .build();
    }
}