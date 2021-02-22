package com.hartwig.pipeline.tertiary.protect;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.bachelor.BachelorOutput;
import com.hartwig.pipeline.tertiary.chord.ChordOutput;
import com.hartwig.pipeline.tertiary.linx.LinxOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

public class Protect implements Stage<ProtectOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "protect";

    private final InputDownload purplePurity;
    private final InputDownload purpleQCFile;
    private final InputDownload purpleDriverCatalog;
    private final InputDownload purpleSomaticVariants;
    private final InputDownload bachelorTsv;
    private final InputDownload linxFusionTsv;
    private final InputDownload linxBreakendTsv;
    private final InputDownload linxDriversTsv;
    private final InputDownload linxViralInsertionsTsv;
    private final InputDownload chordPrediction;
    private final ResourceFiles resourceFiles;

    public Protect(final PurpleOutput purpleOutput, final BachelorOutput bachelorOutput, final LinxOutput linxOutput,
            final ChordOutput chordOutput, final ResourceFiles resourceFiles) {
        this.purplePurity = new InputDownload(purpleOutput.outputLocations().purityTsv());
        this.purpleQCFile = new InputDownload(purpleOutput.outputLocations().qcFile());
        this.purpleDriverCatalog = new InputDownload(purpleOutput.outputLocations().driverCatalog());
        this.purpleSomaticVariants = new InputDownload(purpleOutput.outputLocations().somaticVcf());
        this.bachelorTsv = new InputDownload(bachelorOutput.variants());
        this.linxFusionTsv = new InputDownload(linxOutput.linxOutputLocations().fusions());
        this.linxBreakendTsv = new InputDownload(linxOutput.linxOutputLocations().breakends());
        this.linxDriversTsv = new InputDownload(linxOutput.linxOutputLocations().drivers());
        this.linxViralInsertionsTsv = new InputDownload(linxOutput.linxOutputLocations().viralInsertions());
        this.chordPrediction = new InputDownload(chordOutput.predictions());
        this.resourceFiles = resourceFiles;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(purplePurity,
                purpleQCFile,
                purpleDriverCatalog,
                purpleSomaticVariants,
                bachelorTsv,
                linxFusionTsv,
                linxBreakendTsv,
                linxDriversTsv,
                linxViralInsertionsTsv,
                chordPrediction);
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return List.of(new ProtectCommand(metadata.tumor().sampleName(),
                metadata.tumor().primaryTumorDoids(),
                VmDirectories.OUTPUT,
                resourceFiles.actionabilityDir(),
                resourceFiles.doidJson(),
                resourceFiles.germlineReporting(),
                purplePurity.getLocalTargetPath(),
                purpleQCFile.getLocalTargetPath(),
                purpleDriverCatalog.getLocalTargetPath(),
                purpleSomaticVariants.getLocalTargetPath(),
                bachelorTsv.getLocalTargetPath(),
                linxFusionTsv.getLocalTargetPath(),
                linxBreakendTsv.getLocalTargetPath(),
                linxDriversTsv.getLocalTargetPath(),
                linxViralInsertionsTsv.getLocalTargetPath(),
                chordPrediction.getLocalTargetPath()));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name(namespace())
                .startupCommand(bash)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(4, 12))
                .build();
    }

    @Override
    public ProtectOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return ProtectOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .build();
    }

    @Override
    public ProtectOutput skippedOutput(final SomaticRunMetadata metadata) {
        return ProtectOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && arguments.refGenomeVersion().equals(RefGenomeVersion.V37) && !arguments.shallow()
                && arguments.runGermlineCaller();
    }
}
