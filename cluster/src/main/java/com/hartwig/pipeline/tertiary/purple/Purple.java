package com.hartwig.pipeline.tertiary.purple;

import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.calling.structural.gripss.GripssOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.JavaCommandFactory;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.*;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tertiary.pave.PaveOutput;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.hartwig.pipeline.tools.HmfTool.PURPLE;
import static java.lang.String.format;

@Namespace(Purple.NAMESPACE)
public class Purple implements Stage<PurpleOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "purple";
    public static final String PURPLE_GERMLINE_VCF = ".purple.germline.vcf.gz";
    public static final String PURPLE_SOMATIC_VCF = ".purple.somatic.vcf.gz";
    public static final String PURPLE_SOMATIC_SV_VCF = ".purple.sv.vcf.gz";
    public static final String PURPLE_GERMLINE_SV_VCF = ".purple.sv.germline.vcf.gz";
    public static final String PURPLE_PURITY_TSV = ".purple.purity.tsv";
    public static final String PURPLE_QC = ".purple.qc";
    public static final String PURPLE_SOMATIC_DRIVER_CATALOG = ".purple.driver.catalog.somatic.tsv";
    public static final String PURPLE_GERMLINE_DRIVER_CATALOG = ".purple.driver.catalog.germline.tsv";
    public static final String PURPLE_GERMLINE_DELETION_TSV = ".purple.germline.deletion.tsv";
    public static final String PURPLE_GENE_COPY_NUMBER_TSV = ".purple.cnv.gene.tsv";
    public static final String PURPLE_SOMATIC_COPY_NUMBER_TSV = ".purple.cnv.somatic.tsv";
    public static final String PURPLE_CIRCOS_PLOT = ".circos.png";

    private final ResourceFiles resourceFiles;
    private final InputDownloadCommand somaticVcfDownload;
    private final InputDownloadCommand germlineVcfDownload;
    private final InputDownloadCommand somaticSvVcfDownload;
    private final InputDownloadCommand somaticSvVcfIndexDownload;
    private final InputDownloadCommand germlineSvVcfDownload;
    private final InputDownloadCommand svRecoveryVcfDownload;
    private final InputDownloadCommand svRecoveryVcfIndexDownload;
    private final InputDownloadCommand amberOutputDownload;
    private final InputDownloadCommand cobaltOutputDownload;
    private final PersistedDataset persistedDataset;
    private final Arguments arguments;

    public Purple(final ResourceFiles resourceFiles, final PaveOutput paveSomaticOutput, final PaveOutput paveGermlineOutput,
            final GripssOutput gripssSomaticOutput, final GripssOutput gripssGermlineOutput, final AmberOutput amberOutput,
            final CobaltOutput cobaltOutput, final PersistedDataset persistedDataset, final Arguments arguments) {
        this.resourceFiles = resourceFiles;
        this.somaticVcfDownload = new InputDownloadCommand(paveSomaticOutput.annotatedVariants());
        this.germlineVcfDownload = new InputDownloadCommand(paveGermlineOutput.annotatedVariants());
        this.somaticSvVcfDownload = new InputDownloadCommand(gripssSomaticOutput.filteredVariants());
        this.somaticSvVcfIndexDownload = new InputDownloadCommand(gripssSomaticOutput.filteredVariants().transform(FileTypes::tabixIndex));
        this.svRecoveryVcfDownload = new InputDownloadCommand(gripssSomaticOutput.unfilteredVariants());
        this.svRecoveryVcfIndexDownload = new InputDownloadCommand(gripssSomaticOutput.unfilteredVariants().transform(FileTypes::tabixIndex));
        this.germlineSvVcfDownload = new InputDownloadCommand(gripssGermlineOutput.filteredVariants());
        this.amberOutputDownload = new InputDownloadCommand(amberOutput.outputDirectory());
        this.cobaltOutputDownload = new InputDownloadCommand(cobaltOutput.outputDirectory());
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
                somaticVcfDownload.getLocalTargetPath(),
                somaticSvVcfDownload.getLocalTargetPath(),
                resourceFiles,
                true, svRecoveryVcfDownload.getLocalTargetPath()));

        purpleArguments.addAll(PurpleArguments.germlineArguments(metadata.reference().sampleName(),
                germlineVcfDownload.getLocalTargetPath(), germlineSvVcfDownload.getLocalTargetPath(),
                resourceFiles));

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
                somaticVcfDownload.getLocalTargetPath(),
                somaticSvVcfDownload.getLocalTargetPath(),
                resourceFiles,
                false, svRecoveryVcfDownload.getLocalTargetPath()));
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
                germlineVcfDownload.getLocalTargetPath(), germlineSvVcfDownload.getLocalTargetPath(),
                resourceFiles));
        arguments.add("-no_charts");
        return buildCommand(arguments);
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(somaticVcfDownload,
                somaticSvVcfDownload,
                somaticSvVcfIndexDownload,
                svRecoveryVcfDownload,
                svRecoveryVcfIndexDownload,
                germlineSvVcfDownload,
                amberOutputDownload,
                cobaltOutputDownload,
                germlineVcfDownload);
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.purple(bash, resultsDirectory);
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
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory));

        metadata.maybeTumor().ifPresent(tumor -> {
            final String tumorSampleName = tumor.sampleName();
            String somaticDriverCatalog = somaticDriverCatalog(tumorSampleName);
            String somaticVcf = somaticVcf(tumorSampleName);
            String svVcf = somaticSvVcf(tumorSampleName);
            String geneCopyNumberTsv = geneCopyNumberTsv(tumorSampleName);
            String somaticCopyNumberTsv = somaticCopyNumberTsv(tumorSampleName);

            outputLocationsBuilder.somaticVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticVcf)))
                    .structuralVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(svVcf)))
                    .geneCopyNumber(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(geneCopyNumberTsv)))
                    .somaticCopyNumber(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticCopyNumberTsv)))
                    .somaticDriverCatalog(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticDriverCatalog)))
                    .circosPlot(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(circosPlot(metadata))));
        });

        metadata.maybeReference().ifPresent(reference -> {
            String germlineDeletionTsv = germlineDeletionTsv(metadata.sampleName());
            String germlineVcf = germlineVcf(metadata.sampleName());
            String germlineSvVcf = germlineSvVcf(metadata.sampleName());
            outputLocationsBuilder
                    .germlineVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(germlineVcf)))
                    .germlineStructuralVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(germlineSvVcf)))
                    .germlineDriverCatalog(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(germlineDriverCatalog(metadata.sampleName()))))
                    .germlineDeletions(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(germlineDeletionTsv)));
        });

        outputBuilder.maybeOutputLocations(outputLocationsBuilder.build()).addAllDatatypes(addDatatypes(metadata));
        return outputBuilder.build();
    }

    @Override
    public PurpleOutput persistedOutput(final SomaticRunMetadata metadata) {

        String purityTsv = purityTsv(metadata.sampleName());
        String qcFile = purpleQC(metadata.sampleName());
        final String tumorSampleName = metadata.tumor().sampleName();
        String somaticDriverCatalog = somaticDriverCatalog(tumorSampleName);
        String somaticVcf = somaticVcf(tumorSampleName);
        String somaticSvVcf = somaticSvVcf(tumorSampleName);
        String germlineSvVcf = germlineSvVcf(tumorSampleName);
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
                somaticSvVcf);
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
        GoogleStorageLocation germlineSvsLocation = persistedOrDefault(metadata.tumor().sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.PURPLE_GERMLINE_STRUCTURAL_VARIANTS,
                germlineSvVcf);
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
                .germlineStructuralVariants(germlineSvsLocation)
                .germlineDriverCatalog(germlineDriverCatalogLocation)
                .germlineDeletions(germlineDeletionLocation);

        return PurpleOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .addAllDatatypes(addDatatypes(metadata))
                .maybeOutputLocations(outputLocationsBuilder.build())
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        String purityTsv = purityTsv(metadata.sampleName());
        String qcFile = purpleQC(metadata.sampleName());
        List<AddDatatype> datatypes = new ArrayList<>(List.of(new AddDatatype(DataType.PURPLE_PURITY,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), purityTsv)),
                new AddDatatype(DataType.PURPLE_QC, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), qcFile))));
        metadata.maybeTumor().ifPresent(tumor -> {
            final String tumorSampleName = tumor.sampleName();
            String somaticDriverCatalog = somaticDriverCatalog(tumorSampleName);
            String somaticVcf = somaticVcf(tumorSampleName);
            String somaticSvVcf = somaticSvVcf(tumorSampleName);
            String somaticCopyNumberTsv = somaticCopyNumberTsv(tumorSampleName);
            datatypes.addAll(List.of(new AddDatatype(DataType.SOMATIC_VARIANTS_PURPLE,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), somaticVcf)),
                    new AddDatatype(DataType.STRUCTURAL_VARIANTS_PURPLE,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), somaticSvVcf)),
                    new AddDatatype(DataType.PURPLE_SOMATIC_DRIVER_CATALOG,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), somaticDriverCatalog)),
                    new AddDatatype(DataType.PURPLE_SOMATIC_COPY_NUMBER, metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), somaticCopyNumberTsv)),
                    new AddDatatype(DataType.PURPLE_CIRCOS_PLOT,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), circosPlot(metadata)))));
        });
        metadata.maybeReference().ifPresent(reference -> {
            String germlineDriverCatalog = germlineDriverCatalog(metadata.sampleName());
            String germlineDeletionTsv = germlineDeletionTsv(metadata.sampleName());
            String germlineVcf = germlineVcf(metadata.sampleName());
            String germlineSvVcf = germlineSvVcf(metadata.sampleName());

            datatypes.addAll(List.of(new AddDatatype(DataType.GERMLINE_VARIANTS_PURPLE,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), germlineVcf)),
                    new AddDatatype(DataType.PURPLE_GERMLINE_STRUCTURAL_VARIANTS,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), germlineSvVcf)),
                    new AddDatatype(DataType.PURPLE_GERMLINE_DRIVER_CATALOG,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), germlineDriverCatalog)),
                    new AddDatatype(DataType.PURPLE_GERMLINE_DELETION, metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), germlineDeletionTsv))));
        });
        return datatypes;
    }

    private List<BashCommand> buildCommand(final List<String> arguments) {
        return List.of(JavaCommandFactory.javaJarCommand(PURPLE, arguments));
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

    private static String somaticSvVcf(final String tumorSampleName) { return tumorSampleName + PURPLE_SOMATIC_SV_VCF; }

    private static String germlineSvVcf(final String tumorSampleName) { return tumorSampleName + PURPLE_GERMLINE_SV_VCF; }

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