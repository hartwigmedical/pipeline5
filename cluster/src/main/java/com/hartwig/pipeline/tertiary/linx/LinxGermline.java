package com.hartwig.pipeline.tertiary.linx;

import static com.hartwig.pipeline.metadata.InputMode.REFERENCE_ONLY;
import static com.hartwig.pipeline.metadata.InputMode.TUMOR_REFERENCE;

import java.io.File;
import java.util.Collections;
import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.structural.gripss.GripssOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
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
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

import org.jetbrains.annotations.NotNull;

@Namespace(LinxGermline.NAMESPACE)
public class LinxGermline implements Stage<LinxGermlineOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "linx_germline";
    public static final String GERMLINE_DRIVER_CATALOG_TSV = ".linx.germline.driver.catalog.tsv";
    public static final String GERMLINE_DISRUPTION_TSV = ".linx.germline.disruption.tsv";

    private final InputDownload gripssGermlineVariantsDownload;
    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public LinxGermline(final GripssOutput gripssOutput, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        gripssGermlineVariantsDownload = new InputDownload(gripssOutput.filteredVariants());
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public List<BashCommand> inputs() {
        return Collections.singletonList(gripssGermlineVariantsDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) { return buildCommand(metadata, TUMOR_REFERENCE); }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) { return buildCommand(metadata, REFERENCE_ONLY); }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) { return Stage.disabled(); }

    private List<BashCommand> buildCommand(final SomaticRunMetadata metadata, final InputMode inputMode) {

        List<String> arguments = Lists.newArrayList();

        arguments.add(String.format("-sample %s", metadata.sampleName()));

        arguments.add("-germline");
        arguments.add(String.format("-sv_vcf %s", gripssGermlineVariantsDownload.getLocalTargetPath()));
        arguments.add(String.format("-ref_genome_version %s", resourceFiles.version()));
        arguments.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(String.format("-line_element_file %s", resourceFiles.lineElements()));
        arguments.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        arguments.add(String.format("-driver_gene_panel %s", resourceFiles.driverGenePanel()));
        arguments.add(String.format("-germine_pon_sv_file %s", resourceFiles.svBreakpointPon()));
        arguments.add(String.format("-germine_pon_sgl_file %s", resourceFiles.svBreakendPon()));

        return Collections.singletonList(new JavaJarCommand("linx", Versions.LINX, "linx.jar", "8G", arguments));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.linx("germline", bash, resultsDirectory);
    }

    @Override
    public LinxGermlineOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {

        String disruptionsTsv = metadata.sampleName() + GERMLINE_DISRUPTION_TSV;
        String driverCatalogTsv = metadata.sampleName() + GERMLINE_DRIVER_CATALOG_TSV;

        return LinxGermlineOutput.builder()
                .status(jobStatus)
                .maybeLinxGermlineOutputLocations(LinxGermlineOutputLocations.builder()
                        .disruptions(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(disruptionsTsv)))
                        .driverCatalog(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(driverCatalogTsv)))
                        .outputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                        .build())
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.LINX_GERMLINE_DISRUPTIONS,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), disruptionsTsv)))
                .addDatatypes(new AddDatatype(DataType.LINX_GERMLINE_DRIVER_CATALOG,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), driverCatalogTsv)))
                .build();
    }

    @Override
    public LinxGermlineOutput skippedOutput(final SomaticRunMetadata metadata) {
        return LinxGermlineOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow();
    }

    @Override
    public LinxGermlineOutput persistedOutput(final SomaticRunMetadata metadata) {

        String disruptionsTsv = metadata.sampleName() + GERMLINE_DISRUPTION_TSV;
        String driverCatalogTsv = metadata.sampleName() + GERMLINE_DRIVER_CATALOG_TSV;

        return LinxGermlineOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeLinxGermlineOutputLocations(LinxGermlineOutputLocations.builder()
                        .disruptions(persistedOrDefault(metadata, DataType.LINX_GERMLINE_DISRUPTIONS, disruptionsTsv))
                        .driverCatalog(persistedOrDefault(metadata, DataType.LINX_GERMLINE_DRIVER_CATALOG, driverCatalogTsv))
                        .outputDirectory(persistedOrDefault(metadata,
                                DataType.LINX_DRIVER_CATALOG,
                                driverCatalogTsv).transform(f -> new File(f).getParent()).asDirectory())
                        .build())
                .build();
    }

    @NotNull
    public GoogleStorageLocation persistedOrDefault(final SomaticRunMetadata metadata, final DataType dataType, final String path) {
        return persistedDataset.path(metadata.sampleName(), dataType)
                .orElse(GoogleStorageLocation.of(metadata.bucket(), PersistedLocations.blobForSet(metadata.set(), namespace(), path)));
    }
}
