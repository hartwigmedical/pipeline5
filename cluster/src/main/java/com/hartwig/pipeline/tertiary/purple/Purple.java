package com.hartwig.pipeline.tertiary.purple;

import static java.lang.String.format;

import java.io.File;
import java.util.ArrayList;
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
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tertiary.pave.PaveOutput;
import com.hartwig.pipeline.tools.Versions;

@Namespace(Purple.NAMESPACE)
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
    private final Arguments arguments;

    public Purple(final ResourceFiles resourceFiles, final PaveOutput paveSomaticOutput, final PaveOutput paveGermlineOutput,
            final GripssOutput gripssOutput, final AmberOutput amberOutput, final CobaltOutput cobaltOutput,
            final PersistedDataset persistedDataset, final Arguments arguments) {
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
        this.arguments = arguments;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        List<String> purpleArguments = new ArrayList<>();
        purpleArguments.addAll(PurpleArguments.addCommonArguments(amberOutputDownload.getLocalTargetPath(),
                cobaltOutputDownload.getLocalTargetPath(),
                resourceFiles));
        purpleArguments.addAll(PurpleArguments.tumorArguments(metadata.tumor().sampleName(),
                somaticVariantsDownload.getLocalTargetPath(),
                structuralVariantsDownload.getLocalTargetPath(),
                svRecoveryVariantsDownload.getLocalTargetPath(),
                resourceFiles));
        purpleArguments.addAll(PurpleArguments.germlineArguments(metadata.reference().sampleName(),
                germlineVariantsDownload.getLocalTargetPath(),
                resourceFiles));
        if (arguments.useTargetRegions()) {
            purpleArguments.addAll(PurpleArguments.addTargetRegionsArguments(resourceFiles));
        }
        if (arguments.shallow()) {
            PurpleArguments.addShallowArguments(purpleArguments);
        }
        return buildCommand(purpleArguments);
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        List<String> purpleArguments = new ArrayList<>();
        purpleArguments.addAll(PurpleArguments.addCommonArguments(amberOutputDownload.getLocalTargetPath(),
                cobaltOutputDownload.getLocalTargetPath(),
                resourceFiles));
        purpleArguments.addAll(PurpleArguments.tumorArguments(metadata.tumor().sampleName(),
                somaticVariantsDownload.getLocalTargetPath(),
                structuralVariantsDownload.getLocalTargetPath(),
                svRecoveryVariantsDownload.getLocalTargetPath(),
                resourceFiles));
        if (arguments.useTargetRegions()) {
            purpleArguments.addAll(PurpleArguments.addTargetRegionsArguments(resourceFiles));
        }
        if (arguments.shallow()) {
            PurpleArguments.addShallowArguments(purpleArguments);
        }
        return buildCommand(purpleArguments);
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        List<String> arguments = new ArrayList<>();
        arguments.addAll(PurpleArguments.addCommonArguments(amberOutputDownload.getLocalTargetPath(),
                cobaltOutputDownload.getLocalTargetPath(),
                resourceFiles));
        arguments.addAll(PurpleArguments.germlineArguments(metadata.reference().sampleName(),
                germlineVariantsDownload.getLocalTargetPath(),
                resourceFiles));
        arguments.add("-no_charts");
        return buildCommand(arguments);
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

    @Override
    public PurpleOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {

        String purityTsv = purityTsv(metadata.sampleName());
        String qcFile = purpleQC(metadata.sampleName());

        ImmutablePurpleOutputLocations.Builder outputLocationsBuilder = PurpleOutputLocations.builder()
                .outputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                .purity(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(purityTsv)))
                .qcFile(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(qcFile)));

        ImmutablePurpleOutput.Builder outputBuilder = PurpleOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.PURPLE_PURITY,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), purityTsv)))
                .addDatatypes(new AddDatatype(DataType.PURPLE_QC, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), qcFile)));

        metadata.maybeTumor().ifPresent(tumor -> {
            final String tumorSampleName = tumor.sampleName();
            String somaticDriverCatalog = somaticDriverCatalog(tumorSampleName);
            String somaticVcf = somaticVcf(tumorSampleName);
            String svVcf = svVcf(tumorSampleName);
            String geneCopyNumberTsv = geneCopyNumberTsv(tumorSampleName);
            String somaticCopyNumberTsv = somaticCopyNumberTsv(tumorSampleName);

            outputLocationsBuilder.somaticVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticVcf)))
                    .structuralVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(svVcf)))
                    .geneCopyNumber(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(geneCopyNumberTsv)))
                    .somaticCopyNumber(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticCopyNumberTsv)))
                    .somaticDriverCatalog(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticDriverCatalog)))
                    .circosPlot(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(circosPlot(metadata))));

            outputBuilder.addDatatypes(new AddDatatype(DataType.SOMATIC_VARIANTS_PURPLE,
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
        });

        metadata.maybeReference().ifPresent(reference -> {
            String germlineDriverCatalog = germlineDriverCatalog(metadata.sampleName());
            String germlineDeletionTsv = germlineDeletionTsv(metadata.sampleName());
            String germlineVcf = germlineVcf(metadata.sampleName());
            outputLocationsBuilder.germlineVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(germlineVcf)))
                    .germlineDriverCatalog(GoogleStorageLocation.of(bucket.name(),
                            resultsDirectory.path(germlineDriverCatalog(metadata.sampleName()))))
                    .germlineDeletions(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(germlineDeletionTsv)));

            outputBuilder.addDatatypes(new AddDatatype(DataType.GERMLINE_VARIANTS_PURPLE,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), germlineVcf)))
                    .addDatatypes(new AddDatatype(DataType.PURPLE_GERMLINE_DRIVER_CATALOG,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), germlineDriverCatalog)))
                    .addDatatypes(new AddDatatype(DataType.PURPLE_GERMLINE_DELETION,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), germlineDeletionTsv)));
        });

        outputBuilder.maybeOutputLocations(outputLocationsBuilder.build());
        return outputBuilder.build();
    }

    @Override
    public PurpleOutput persistedOutput(final SomaticRunMetadata metadata) {

        String purityTsv = purityTsv(metadata.sampleName());
        String qcFile = purpleQC(metadata.sampleName());
        final String tumorSampleName = metadata.tumor().sampleName();
        String somaticDriverCatalog = somaticDriverCatalog(tumorSampleName);
        String somaticVcf = somaticVcf(tumorSampleName);
        String svVcf = svVcf(tumorSampleName);
        String geneCopyNumberTsv = geneCopyNumberTsv(tumorSampleName);
        String somaticCopyNumberTsv = somaticCopyNumberTsv(tumorSampleName);

        String germlineDriverCatalog = germlineDriverCatalog(metadata.sampleName());
        String germlineDeletionTsv = germlineDeletionTsv(metadata.sampleName());
        String germlineVcf = germlineVcf(metadata.sampleName());

        GoogleStorageLocation purityLocation =
                persistedOrDefault(metadata.sampleName(), metadata.set(), metadata.bucket(), DataType.PURPLE_PURITY, purityTsv);
        GoogleStorageLocation qcLocation =
                persistedOrDefault(metadata.sampleName(), metadata.set(), metadata.bucket(), DataType.PURPLE_QC, qcFile);

        ImmutablePurpleOutputLocations.Builder outputLocationsBuilder = PurpleOutputLocations.builder()
                .outputDirectory(purityLocation.transform(f -> new File(f).getParent()).asDirectory())
                .purity(purityLocation)
                .qcFile(qcLocation);

        GoogleStorageLocation somaticVariantsLocation = persistedOrDefault(metadata.tumor().sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.SOMATIC_VARIANTS_PURPLE,
                somaticVcf);
        GoogleStorageLocation svsLocation = persistedOrDefault(metadata.tumor().sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.STRUCTURAL_VARIANTS_PURPLE,
                svVcf);
        GoogleStorageLocation geneCopyNumberLocation = persistedOrDefault(metadata.tumor().sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.PURPLE_GENE_COPY_NUMBER,
                geneCopyNumberTsv);
        GoogleStorageLocation somaticDriverCatalogLocation = persistedOrDefault(metadata.tumor().sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.PURPLE_SOMATIC_DRIVER_CATALOG,
                somaticDriverCatalog);
        GoogleStorageLocation somaticCopyNumberLocation = persistedOrDefault(metadata.tumor().sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.PURPLE_SOMATIC_COPY_NUMBER,
                somaticCopyNumberTsv);
        GoogleStorageLocation circosPlot = persistedOrDefault(metadata.tumor().sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.PURPLE_CIRCOS_PLOT,
                circosPlot(metadata));
        outputLocationsBuilder.somaticVariants(somaticVariantsLocation)
                .structuralVariants(svsLocation)
                .purity(purityLocation)
                .qcFile(qcLocation)
                .geneCopyNumber(geneCopyNumberLocation)
                .somaticDriverCatalog(somaticDriverCatalogLocation)
                .circosPlot(circosPlot)
                .somaticCopyNumber(somaticCopyNumberLocation);

        GoogleStorageLocation germlineVariantsLocation = persistedOrDefault(metadata.sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.GERMLINE_VARIANTS_PURPLE,
                germlineVcf);
        GoogleStorageLocation germlineDriverCatalogLocation = persistedOrDefault(metadata.sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.PURPLE_GERMLINE_DRIVER_CATALOG,
                germlineDriverCatalog);
        GoogleStorageLocation germlineDeletionLocation = persistedOrDefault(metadata.sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.PURPLE_GERMLINE_DELETION,
                germlineDeletionTsv);

        outputLocationsBuilder.germlineVariants(germlineVariantsLocation)
                .germlineDriverCatalog(germlineDriverCatalogLocation)
                .germlineDeletions(germlineDeletionLocation);

        return PurpleOutput.builder().status(PipelineStatus.PERSISTED).maybeOutputLocations(outputLocationsBuilder.build()).build();
    }

    private List<BashCommand> buildCommand(final List<String> arguments) {
        return Collections.singletonList(new JavaJarCommand("purple", Versions.PURPLE, "purple.jar", "28G", arguments));
    }

    private GoogleStorageLocation persistedOrDefault(final String sample, final String set, final String bucket,
            final DataType somaticVariantsPurple, final String s) {
        return persistedDataset.path(sample, somaticVariantsPurple)
                .orElse(GoogleStorageLocation.of(bucket, PersistedLocations.blobForSet(set, namespace(), s)));
    }

    private static String circosPlot(final SomaticRunMetadata metadata) {
        return format("plot/%s%s", metadata.tumor().sampleName(), PURPLE_CIRCOS_PLOT);
    }

    private static String purpleQC(final String sample) {
        return sample + PURPLE_QC;
    }

    private static String purityTsv(final String sample) {
        return sample + PURPLE_PURITY_TSV;
    }

    private static String somaticCopyNumberTsv(final String tumorSampleName) {
        return tumorSampleName + PURPLE_SOMATIC_COPY_NUMBER_TSV;
    }

    private static String svVcf(final String tumorSampleName) {
        return tumorSampleName + PURPLE_SV_VCF;
    }

    private static String somaticVcf(final String tumorSampleName) {
        return tumorSampleName + PURPLE_SOMATIC_VCF;
    }

    private static String somaticDriverCatalog(final String tumorSampleName) {
        return tumorSampleName + PURPLE_SOMATIC_DRIVER_CATALOG;
    }

    private static String geneCopyNumberTsv(final String tumorSampleName) {
        return tumorSampleName + PURPLE_GENE_COPY_NUMBER_TSV;
    }

    private static String germlineVcf(final String sample) {
        return sample + PURPLE_GERMLINE_VCF;
    }

    private static String germlineDriverCatalog(final String sample) {
        return sample + PURPLE_GERMLINE_DRIVER_CATALOG;
    }

    private static String germlineDeletionTsv(final String sample) {
        return sample + PURPLE_GERMLINE_DELETION_TSV;
    }
}