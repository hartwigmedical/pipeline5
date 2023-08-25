package com.hartwig.pipeline.tertiary.linx;

import com.google.api.client.util.Lists;
import com.hartwig.computeengine.execution.ComputeEngineStatus;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.execution.vm.command.java.JavaJarCommand;
import com.hartwig.computeengine.input.SomaticRunMetadata;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.datatypes.DataType;
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

@Namespace(LinxGermline.NAMESPACE)
public class LinxGermline implements Stage<LinxGermlineOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "linx_germline";
    public static final String GERMLINE_DRIVER_CATALOG_TSV = ".linx.germline.driver.catalog.tsv";
    public static final String GERMLINE_DISRUPTION_TSV = ".linx.germline.disruption.tsv";
    public static final String GERMLINE_BREAKEND_TSV = ".linx.germline.breakend.tsv";

    private final InputDownloadCommand purpleGermlineSvsDownload;
    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public LinxGermline(final PurpleOutput purpleOutput, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        PurpleOutputLocations purpleOutputLocations = purpleOutput.outputLocations();
        purpleGermlineSvsDownload = new InputDownloadCommand(purpleOutputLocations.germlineStructuralVariants().isPresent()
                ? purpleOutputLocations.germlineStructuralVariants().get()
                : GoogleStorageLocation.empty());
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public List<BashCommand> inputs() {
        return Collections.singletonList(purpleGermlineSvsDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        return buildCommand(metadata);
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        return buildCommand(metadata);
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("linx-" + "germline")
                .startupCommand(bash)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(4, 12))
                .workingDiskSpaceGb(VirtualMachineJobDefinition.LOCAL_SSD_DISK_SPACE_GB)
                .build();
    }

    @Override
    public LinxGermlineOutput output(final SomaticRunMetadata metadata, final ComputeEngineStatus jobStatus, final RuntimeBucket bucket,
                                     final ResultsDirectory resultsDirectory) {
        return LinxGermlineOutput.builder()
                .status(jobStatus)
                .maybeLinxGermlineOutputLocations(LinxGermlineOutputLocations.builder()
                        .disruptions(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(disruptionsTsv(metadata))))
                        .breakends(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(breakendsTsv(metadata))))
                        .driverCatalog(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(driverCatalogTsv(metadata))))
                        .outputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                        .build())
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public LinxGermlineOutput skippedOutput(final SomaticRunMetadata metadata) {
        return LinxGermlineOutput.builder().status(ComputeEngineStatus.SKIPPED).build();
    }

    @Override
    public LinxGermlineOutput persistedOutput(final SomaticRunMetadata metadata) {
        String driverCatalogTsv = driverCatalogTsv(metadata);
        return LinxGermlineOutput.builder()
                .status(ComputeEngineStatus.PERSISTED)
                .maybeLinxGermlineOutputLocations(LinxGermlineOutputLocations.builder()
                        .disruptions(persistedOrDefault(metadata, DataType.LINX_GERMLINE_DISRUPTIONS, disruptionsTsv(metadata)))
                        .breakends(persistedOrDefault(metadata, DataType.LINX_GERMLINE_BREAKENDS, breakendsTsv(metadata)))
                        .driverCatalog(persistedOrDefault(metadata, DataType.LINX_GERMLINE_DRIVER_CATALOG, driverCatalogTsv))
                        .outputDirectory(persistedOrDefault(metadata,
                                DataType.LINX_DRIVER_CATALOG,
                                driverCatalogTsv).transform(f -> new File(f).getParent()).asDirectory())
                        .build())
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        return List.of(new AddDatatype(DataType.LINX_GERMLINE_DISRUPTIONS,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), disruptionsTsv(metadata))),
                new AddDatatype(DataType.LINX_GERMLINE_BREAKENDS,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), breakendsTsv(metadata))),
                new AddDatatype(DataType.LINX_GERMLINE_DRIVER_CATALOG,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), driverCatalogTsv(metadata))));
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow();
    }

    private List<BashCommand> buildCommand(final SomaticRunMetadata metadata) {

        List<String> arguments = Lists.newArrayList();

        arguments.add(String.format("-sample %s", metadata.sampleName()));

        arguments.add("-germline");
        arguments.add(String.format("-sv_vcf %s", purpleGermlineSvsDownload.getLocalTargetPath()));
        arguments.add(String.format("-ref_genome_version %s", resourceFiles.version()));
        arguments.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        arguments.add(String.format("-driver_gene_panel %s", resourceFiles.driverGenePanel()));
        return Collections.singletonList(new JavaJarCommand(LINX.getToolName(), LINX.getVersion(), LINX.jar(), LINX.maxHeapStr(), arguments));
    }

    private String driverCatalogTsv(final SomaticRunMetadata metadata) {
        return metadata.sampleName() + GERMLINE_DRIVER_CATALOG_TSV;
    }

    private String disruptionsTsv(final SomaticRunMetadata metadata) {
        return metadata.sampleName() + GERMLINE_DISRUPTION_TSV;
    }

    private String breakendsTsv(final SomaticRunMetadata metadata) {
        return metadata.sampleName() + GERMLINE_BREAKEND_TSV;
    }

    @NotNull
    public GoogleStorageLocation persistedOrDefault(final SomaticRunMetadata metadata, final DataType dataType, final String path) {
        return persistedDataset.path(metadata.sampleName(), dataType)
                .orElse(GoogleStorageLocation.of(metadata.bucket(), PersistedLocations.blobForSet(metadata.set(), namespace(), path)));
    }
}
