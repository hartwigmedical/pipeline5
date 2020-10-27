package com.hartwig.pipeline.tertiary.linx;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

public class Linx implements Stage<LinxOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "linx";
    public static final String KNOWLEDGEBASE_OUTPUT = "output/";

    private final InputDownload purpleOutputDirDownload;
    private final InputDownload purpleStructuralVcfDownload;
    private final ResourceFiles resourceFiles;

    public Linx(PurpleOutput purpleOutput, final ResourceFiles resourceFiles) {
        purpleOutputDirDownload = new InputDownload(purpleOutput.outputDirectory());
        purpleStructuralVcfDownload = new InputDownload(purpleOutput.structuralVcf());
        this.resourceFiles = resourceFiles;
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
        return Collections.singletonList(new LinxCommand(metadata.tumor().sampleName(),
                purpleStructuralVcfDownload.getLocalTargetPath(),
                purpleOutputDirDownload.getLocalTargetPath(),
                resourceFiles.refGenomeFile(),
                resourceFiles.version(),
                VmDirectories.OUTPUT,
                resourceFiles.fragileSites(),
                resourceFiles.lineElements(),
                resourceFiles.originsOfReplication(),
                resourceFiles.viralHostRefs(),
                resourceFiles.ensemblDataCache(),
                resourceFiles.knownFusionData(),
                resourceFiles.driverGenePanel()));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.linx(bash, resultsDirectory);
    }

    @Override
    public LinxOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return LinxOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory))
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
}
