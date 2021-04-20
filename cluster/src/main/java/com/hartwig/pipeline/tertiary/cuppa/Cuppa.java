package com.hartwig.pipeline.tertiary.cuppa;

import static java.lang.String.format;
import static java.lang.String.join;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
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
import com.hartwig.pipeline.tertiary.linx.LinxOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tools.Versions;

public class Cuppa implements Stage<CuppaOutput, SomaticRunMetadata> {
    static String NAMESPACE = "cuppa";
    private final InputDownload purpleSomaticVcfDownload;
    private final InputDownload purpleStructuralVcfDownload;
    private final InputDownload purpleQc;
    private final InputDownload purplePurity;
    private final InputDownload linxDriverCatalog;
    private final InputDownload linxClusters;
    private final InputDownload linxFusions;
    private final InputDownload linxViralInsertions;
    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public Cuppa(final PurpleOutput purpleOutput, final LinxOutput linxOutput, final ResourceFiles resourceFiles,
            final PersistedDataset persistedDataset) {
        purpleSomaticVcfDownload = new InputDownload(purpleOutput.outputLocations().somaticVcf());
        purpleStructuralVcfDownload = new InputDownload(purpleOutput.outputLocations().structuralVcf());
        purpleQc = new InputDownload(purpleOutput.outputLocations().qcFile());
        purplePurity = new InputDownload(purpleOutput.outputLocations().purityTsv());
        linxDriverCatalog = new InputDownload(linxOutput.linxOutputLocations().driverCatalog());
        linxClusters = new InputDownload(linxOutput.linxOutputLocations().breakends());
        linxFusions = new InputDownload(linxOutput.linxOutputLocations().fusions());
        linxViralInsertions = new InputDownload(linxOutput.linxOutputLocations().viralInsertions());
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(purpleSomaticVcfDownload,
                purpleStructuralVcfDownload,
                purpleQc,
                purplePurity,
                linxDriverCatalog,
                linxClusters,
                linxFusions,
                linxViralInsertions);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return List.of(new JavaJarCommand("cuppa",
                        Versions.CUPPA,
                        "cuppa.jar",
                        "4G",
                        List.of("-categories",
                                "DNA",
                                "-ref_data_dir",
                                resourceFiles.cuppaRefData(),
                                "-sample_data",
                                metadata.tumor().sampleName(),
                                "-sample_data_dir",
                                VmDirectories.INPUT,
                                "-sample_sv_file",
                                purpleStructuralVcfDownload.getLocalTargetPath(),
                                "-sample_somatic_vcf",
                                purpleSomaticVcfDownload.getLocalTargetPath(),
                                "-log_debug",
                                "-output_dir",
                                VmDirectories.OUTPUT)),
                () -> join(" ",
                        List.of("/usr/bin/python3",
                                VmDirectories.toolPath(format("cuppa/%s/cuppa_chart.py", Versions.CUPPA)),
                                "-sample",
                                metadata.tumor().sampleName(),
                                "-sample_data",
                                VmDirectories.outputFile(format("%s.cup.data.csv", metadata.tumor().sampleName())),
                                "-output_dir",
                                VmDirectories.OUTPUT)));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .name(NAMESPACE)
                .startupCommand(bash)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(4, 16))
                .build();
    }

    @Override
    public CuppaOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return CuppaOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.CUPPA_CHART,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), chartPath(metadata.tumor().sampleName()))))
                .build();
    }

    @Override
    public CuppaOutput skippedOutput(final SomaticRunMetadata metadata) {
        return CuppaOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public CuppaOutput persistedOutput(final SomaticRunMetadata metadata) {
        String sample = metadata.tumor().sampleName();
        return CuppaOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeChart(persistedDataset.path(sample, DataType.CUPPA_CHART)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), chartPath(sample)))))
                .build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return !arguments.shallow() && arguments.runTertiary() && arguments.runCuppa();
    }

    private String chartPath(String sampleName) {
        return sampleName + ".cuppa.chart.png";
    }
}
