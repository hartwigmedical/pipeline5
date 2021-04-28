package com.hartwig.pipeline.tertiary.cuppa;

import static java.lang.String.format;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.execution.vm.python.Python3Command;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
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
    private final InputDownload purpleOutputDirectory;
    private final InputDownload linxOutputDirectory;

    private final ResourceFiles resourceFiles;

    public Cuppa(final PurpleOutput purpleOutput, final LinxOutput linxOutput, final ResourceFiles resourceFiles) {
        purpleSomaticVcfDownload = new InputDownload(purpleOutput.outputLocations().somaticVcf());
        purpleStructuralVcfDownload = new InputDownload(purpleOutput.outputLocations().structuralVcf());
        purpleOutputDirectory = new InputDownload(purpleOutput.outputLocations().outputDirectory());
        linxOutputDirectory = new InputDownload(linxOutput.linxOutputLocations().outputDirectory());
        this.resourceFiles = resourceFiles;
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(purpleSomaticVcfDownload, purpleStructuralVcfDownload, purpleOutputDirectory, linxOutputDirectory);
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
                                linxOutputDirectory.getLocalTargetPath(),
                                "-sample_sv_file",
                                purpleStructuralVcfDownload.getLocalTargetPath(),
                                "-sample_somatic_vcf",
                                purpleSomaticVcfDownload.getLocalTargetPath(),
                                "-log_debug",
                                "-output_dir",
                                VmDirectories.OUTPUT)),
                new Python3Command("cuppa-chart",
                        Versions.CUPPA,
                        "cuppa-chart.py",
                        List.of("-sample",
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
                .build();
    }

    @Override
    public CuppaOutput skippedOutput(final SomaticRunMetadata metadata) {
        return CuppaOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return !arguments.shallow() && arguments.runTertiary();
    }
}
