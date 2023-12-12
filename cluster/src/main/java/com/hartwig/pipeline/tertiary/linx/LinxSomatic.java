package com.hartwig.pipeline.tertiary.linx;

import com.google.api.client.util.Lists;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.JavaCommandFactory;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.*;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutputLocations;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Collections;
import java.util.List;

import static com.hartwig.computeengine.execution.vm.VirtualMachinePerformanceProfile.custom;
import static com.hartwig.pipeline.tools.HmfTool.LINX;

@Namespace(LinxSomatic.NAMESPACE)
public class LinxSomatic implements Stage<LinxSomaticOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "linx";
    public static final String BREAKEND_TSV = ".linx.breakend.tsv";
    public static final String CLUSTERS_TSV = ".linx.clusters.tsv";
    public static final String SV_ANNOTATIONS_TSV = ".linx.svs.tsv";
    public static final String DRIVER_CATALOG_TSV = ".linx.driver.catalog.tsv";
    public static final String FUSION_TSV = ".linx.fusion.tsv";
    public static final String DRIVERS_TSV = ".linx.drivers.tsv";

    private final InputDownloadCommand purpleOutputDirDownload;
    private final InputDownloadCommand purpleStructuralVariantsDownload;
    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public LinxSomatic(final PurpleOutput purpleOutput, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        PurpleOutputLocations purpleOutputLocations = purpleOutput.outputLocations();
        purpleOutputDirDownload = new InputDownloadCommand(purpleOutputLocations.outputDirectory());
        purpleStructuralVariantsDownload = new InputDownloadCommand(purpleOutputLocations.structuralVariants().isPresent()
                ? purpleOutputLocations.structuralVariants().get()
                : GoogleStorageLocation.empty());
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public List<BashCommand> inputs() {
        return Collections.singletonList(purpleOutputDirDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        return buildCommands(metadata);
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        return buildCommands(metadata);
    }

    private List<BashCommand> buildCommands(final SomaticRunMetadata metadata) {

        List<String> arguments = Lists.newArrayList();

        arguments.add(String.format("-sample %s", metadata.tumor().sampleName()));
        arguments.add(String.format("-sv_vcf %s", purpleStructuralVariantsDownload.getLocalTargetPath()));
        arguments.add(String.format("-purple_dir %s", purpleOutputDirDownload.getLocalTargetPath()));
        arguments.add(String.format("-ref_genome_version %s", resourceFiles.version()));
        arguments.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        arguments.add(String.format("-known_fusion_file %s", resourceFiles.knownFusionData()));
        arguments.add(String.format("-driver_gene_panel %s", resourceFiles.driverGenePanel()));
        arguments.add("-write_vis_data");

        return List.of(JavaCommandFactory.javaJarCommand(LINX, arguments),
                new LinxVisualisationsCommand(metadata.tumor().sampleName(), VmDirectories.OUTPUT, resourceFiles.version()));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .imageFamily(IMAGE_FAMILY)
                .name("linx-" + "somatic")
                .startupCommand(bash)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(4, 12))
                .workingDiskSpaceGb(VirtualMachineJobDefinition.LOCAL_SSD_DISK_SPACE_GB)
                .build();
    }

    @Override
    public LinxSomaticOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
                                    final ResultsDirectory resultsDirectory) {

        String breakendTsv = metadata.tumor().sampleName() + BREAKEND_TSV;
        String driverCatalogTsv = metadata.tumor().sampleName() + DRIVER_CATALOG_TSV;
        String fusionsTsv = metadata.tumor().sampleName() + FUSION_TSV;
        String driversTsv = metadata.tumor().sampleName() + DRIVERS_TSV;
        String clustersTsv = metadata.tumor().sampleName() + CLUSTERS_TSV;
        String svAnnotationsTsv = metadata.tumor().sampleName() + SV_ANNOTATIONS_TSV;

        return LinxSomaticOutput.builder()
                .status(jobStatus)
                .maybeLinxOutputLocations(LinxSomaticOutputLocations.builder()
                        .breakends(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(breakendTsv)))
                        .drivers(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(driversTsv)))
                        .driverCatalog(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(driverCatalogTsv)))
                        .fusions(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(fusionsTsv)))
                        .svAnnotations(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(svAnnotationsTsv)))
                        .clusters(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(clustersTsv)))
                        .outputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                        .build())
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        String breakendTsv = metadata.tumor().sampleName() + BREAKEND_TSV;
        String driverCatalogTsv = metadata.tumor().sampleName() + DRIVER_CATALOG_TSV;
        String fusionsTsv = metadata.tumor().sampleName() + FUSION_TSV;
        String driversTsv = metadata.tumor().sampleName() + DRIVERS_TSV;
        String clustersTsv = metadata.tumor().sampleName() + CLUSTERS_TSV;
        String svAnnotationsTsv = metadata.tumor().sampleName() + SV_ANNOTATIONS_TSV;
        return List.of(new AddDatatype(DataType.LINX, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), driversTsv), true),
                new AddDatatype(DataType.LINX_BREAKENDS, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), breakendTsv)),
                new AddDatatype(DataType.LINX_DRIVER_CATALOG,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), driverCatalogTsv)),
                new AddDatatype(DataType.LINX_DRIVERS, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), driversTsv)),
                new AddDatatype(DataType.LINX_FUSIONS, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), fusionsTsv)),
                new AddDatatype(DataType.LINX_CLUSTERS, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), clustersTsv)),
                new AddDatatype(DataType.LINX_SV_ANNOTATIONS,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), svAnnotationsTsv)));
    }

    @Override
    public LinxSomaticOutput skippedOutput(final SomaticRunMetadata metadata) {
        return LinxSomaticOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow();
    }

    @Override
    public LinxSomaticOutput persistedOutput(final SomaticRunMetadata metadata) {
        String breakendTsv = metadata.tumor().sampleName() + BREAKEND_TSV;
        String driverCatalogTsv = metadata.tumor().sampleName() + DRIVER_CATALOG_TSV;
        String fusionsTsv = metadata.tumor().sampleName() + FUSION_TSV;
        String driversTsv = metadata.tumor().sampleName() + DRIVERS_TSV;
        String clustersTsv = metadata.tumor().sampleName() + CLUSTERS_TSV;
        String svAnnotationsTsv = metadata.tumor().sampleName() + SV_ANNOTATIONS_TSV;
        return LinxSomaticOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeLinxOutputLocations(LinxSomaticOutputLocations.builder()
                        .breakends(persistedOrDefault(metadata, DataType.LINX_BREAKENDS, breakendTsv))
                        .driverCatalog(persistedOrDefault(metadata, DataType.LINX_DRIVER_CATALOG, driverCatalogTsv))
                        .drivers(persistedOrDefault(metadata, DataType.LINX_DRIVERS, driversTsv))
                        .fusions(persistedOrDefault(metadata, DataType.LINX_FUSIONS, fusionsTsv))
                        .svAnnotations(persistedOrDefault(metadata, DataType.LINX_SV_ANNOTATIONS, svAnnotationsTsv))
                        .clusters(persistedOrDefault(metadata, DataType.LINX_CLUSTERS, clustersTsv))
                        .outputDirectory(persistedOrDefault(metadata,
                                DataType.LINX_DRIVER_CATALOG,
                                driverCatalogTsv).transform(f -> new File(f).getParent()).asDirectory())
                        .build())
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @NotNull
    public GoogleStorageLocation persistedOrDefault(final SomaticRunMetadata metadata, final DataType dataType, final String path) {
        return persistedDataset.path(metadata.tumor().sampleName(), dataType)
                .orElse(GoogleStorageLocation.of(metadata.bucket(), PersistedLocations.blobForSet(metadata.set(), namespace(), path)));
    }
}
