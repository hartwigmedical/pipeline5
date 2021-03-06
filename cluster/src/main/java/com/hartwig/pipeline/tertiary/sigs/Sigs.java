package com.hartwig.pipeline.tertiary.sigs;

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
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.sigs.SigsOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tools.Versions;

public class Sigs implements Stage<SigsOutput, SomaticRunMetadata> {
    static String NAMESPACE = "sigs";

    private final InputDownload purpleSomaticVcfDownload;

    private final ResourceFiles resourceFiles;

    public Sigs(final PurpleOutput purpleOutput, final ResourceFiles resourceFiles) {
        purpleSomaticVcfDownload = new InputDownload(purpleOutput.outputLocations().somaticVcf());
        this.resourceFiles = resourceFiles;
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(purpleSomaticVcfDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return List.of(new JavaJarCommand("sigs",
                        Versions.SIGS,
                        "sigs.jar",
                        "4G",
                        List.of("-sample",
                                metadata.tumor().sampleName(),
                                "-signatures_file",
                                resourceFiles.snvSignatures(),
                                "-somatic_vcf_file",
                                purpleSomaticVcfDownload.getLocalTargetPath(),
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
    public SigsOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return SigsOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .build();
    }

    @Override
    public SigsOutput skippedOutput(final SomaticRunMetadata metadata) {
        return SigsOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return !arguments.shallow() && arguments.runTertiary();
    }
}
