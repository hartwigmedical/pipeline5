package com.hartwig.pipeline.tertiary.linx;

import java.io.File;
import java.util.Collections;
import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
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
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

import org.jetbrains.annotations.NotNull;

public class Linx implements Stage<LinxOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "linx";
    public static final String BREAKEND_TSV = ".linx.breakend.tsv";
    public static final String CLUSTERS_TSV = ".linx.clusters.tsv";
    public static final String DRIVER_CATALOG_TSV = ".linx.driver.catalog.tsv";
    public static final String FUSION_TSV = ".linx.fusion.tsv";
    public static final String VIRAL_INSERTS_TSV = ".linx.viral_inserts.tsv";
    public static final String DRIVERS_TSV = ".linx.drivers.tsv";

    private final InputDownload purpleOutputDirDownload;
    private final InputDownload purpleStructuralVcfDownload;
    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public Linx(PurpleOutput purpleOutput, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        purpleOutputDirDownload = new InputDownload(purpleOutput.outputLocations().outputDirectory());
        purpleStructuralVcfDownload = new InputDownload(purpleOutput.outputLocations().structuralVcf());
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
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {

        List<BashCommand> commands = Lists.newArrayList();

        commands.add(new LinxCommand(metadata.tumor().sampleName(),
                purpleStructuralVcfDownload.getLocalTargetPath(),
                purpleOutputDirDownload.getLocalTargetPath(),
                resourceFiles.version(),
                VmDirectories.OUTPUT,
                resourceFiles.fragileSites(),
                resourceFiles.lineElements(),
                resourceFiles.ensemblDataCache(),
                resourceFiles.knownFusionData(),
                resourceFiles.driverGenePanel()));

        commands.add(new LinxVisualisationsCommand(metadata.tumor().sampleName(), VmDirectories.OUTPUT, resourceFiles.version()));

        return commands;
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.linx(bash, resultsDirectory);
    }

    @Override
    public LinxOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {

        String breakendTsv = metadata.tumor().sampleName() + BREAKEND_TSV;
        String driverCatalogTsv = metadata.tumor().sampleName() + DRIVER_CATALOG_TSV;
        String fusionsTsv = metadata.tumor().sampleName() + FUSION_TSV;
        String driversTsv = metadata.tumor().sampleName() + DRIVERS_TSV;

        return LinxOutput.builder()
                .status(jobStatus)
                .maybeLinxOutputLocations(LinxOutputLocations.builder()
                        .breakends(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(breakendTsv)))
                        .drivers(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(driversTsv)))
                        .driverCatalog(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(driverCatalogTsv)))
                        .fusions(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(fusionsTsv)))
                        .outputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                        .build())
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.LINX, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), driversTsv)))
                .addDatatypes(new AddDatatype(DataType.LINX_BREAKENDS,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), breakendTsv)))
                .addDatatypes(new AddDatatype(DataType.LINX_DRIVER_CATALOG,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), driverCatalogTsv)))
                .addDatatypes(new AddDatatype(DataType.LINX_FUSIONS,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), fusionsTsv)))
                .build();
    }

    @Override
    public LinxOutput skippedOutput(final SomaticRunMetadata metadata) {
        return LinxOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow();
    }

    @Override
    public LinxOutput persistedOutput(final SomaticRunMetadata metadata) {
        String breakendTsv = metadata.tumor().sampleName() + BREAKEND_TSV;
        String driverCatalogTsv = metadata.tumor().sampleName() + DRIVER_CATALOG_TSV;
        String fusionsTsv = metadata.tumor().sampleName() + FUSION_TSV;
        String driversTsv = metadata.tumor().sampleName() + DRIVERS_TSV;
        return LinxOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeLinxOutputLocations(LinxOutputLocations.builder()
                        .breakends(persistedOrDefault(metadata, DataType.LINX_DRIVER_CATALOG, breakendTsv))
                        .driverCatalog(persistedOrDefault(metadata, DataType.LINX_DRIVER_CATALOG, driverCatalogTsv))
                        .drivers(persistedOrDefault(metadata, DataType.LINX_DRIVER_CATALOG, driversTsv))
                        .fusions(persistedOrDefault(metadata, DataType.LINX_DRIVER_CATALOG, fusionsTsv))
                        .outputDirectory(persistedOrDefault(metadata,
                                DataType.LINX_DRIVER_CATALOG,
                                driverCatalogTsv).transform(f -> new File(f).getParent()).asDirectory())
                        .build())
                .build();
    }

    @NotNull
    public GoogleStorageLocation persistedOrDefault(final SomaticRunMetadata metadata, final DataType dataType, final String path) {
        return persistedDataset.path(metadata.tumor().sampleName(), dataType)
                .orElse(GoogleStorageLocation.of(metadata.bucket(), PersistedLocations.blobForSet(metadata.set(), namespace(), path)));
    }
}
