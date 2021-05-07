package com.hartwig.pipeline.tertiary.protect;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.chord.ChordOutput;
import com.hartwig.pipeline.tertiary.linx.LinxOutput;
import com.hartwig.pipeline.tertiary.linx.LinxOutputLocations;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

public class Protect implements Stage<ProtectOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "protect";
    private static final String PROTECT_EVIDENCE_TSV = ".protect.tsv";

    private final InputDownload purplePurity;
    private final InputDownload purpleQCFile;
    private final InputDownload purpleSomaticDriverCatalog;
    private final InputDownload purpleGermlineDriverCatalog;
    private final InputDownload purpleSomaticVariants;
    private final InputDownload purpleGermlineVariants;
    private final InputDownload linxFusionTsv;
    private final InputDownload linxBreakendTsv;
    private final InputDownload linxDriverCatalogTsv;
    private final InputDownload chordPrediction;
    private final ResourceFiles resourceFiles;

    public Protect(final PurpleOutput purpleOutput, final LinxOutput linxOutput, final ChordOutput chordOutput,
            final ResourceFiles resourceFiles) {
        this.purplePurity = new InputDownload(purpleOutput.outputLocations().purityTsv());
        this.purpleQCFile = new InputDownload(purpleOutput.outputLocations().qcFile());
        this.purpleSomaticDriverCatalog = new InputDownload(purpleOutput.outputLocations().somaticDriverCatalog());
        this.purpleGermlineDriverCatalog = new InputDownload(purpleOutput.outputLocations().germlineDriverCatalog());
        this.purpleSomaticVariants = new InputDownload(purpleOutput.outputLocations().somaticVcf());
        this.purpleGermlineVariants = new InputDownload(purpleOutput.outputLocations().germlineVcf());
        this.linxFusionTsv = new InputDownload(linxOutput.maybeLinxOutputLocations()
                .map(LinxOutputLocations::fusions)
                .orElse(GoogleStorageLocation.empty()));
        this.linxBreakendTsv = new InputDownload(linxOutput.maybeLinxOutputLocations()
                .map(LinxOutputLocations::breakends)
                .orElse(GoogleStorageLocation.empty()));
        this.linxDriverCatalogTsv = new InputDownload(linxOutput.maybeLinxOutputLocations()
                .map(LinxOutputLocations::driverCatalog)
                .orElse(GoogleStorageLocation.empty()));
        this.chordPrediction = new InputDownload(chordOutput.maybePredictions().orElse(GoogleStorageLocation.empty()));
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
                purpleSomaticDriverCatalog,
                purpleGermlineDriverCatalog,
                purpleSomaticVariants,
                purpleGermlineVariants,
                linxFusionTsv,
                linxBreakendTsv,
                linxDriverCatalogTsv,
                chordPrediction);
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return List.of(new ProtectCommand(metadata.tumor().sampleName(),
                metadata.tumor().primaryTumorDoids(),
                VmDirectories.OUTPUT,
                resourceFiles.actionabilityDir(),
                resourceFiles.doidJson(),
                purplePurity.getLocalTargetPath(),
                purpleQCFile.getLocalTargetPath(),
                purpleSomaticDriverCatalog.getLocalTargetPath(),
                purpleGermlineDriverCatalog.getLocalTargetPath(),
                purpleSomaticVariants.getLocalTargetPath(),
                purpleGermlineVariants.getLocalTargetPath(),
                linxFusionTsv.getLocalTargetPath(),
                linxBreakendTsv.getLocalTargetPath(),
                linxDriverCatalogTsv.getLocalTargetPath(),
                chordPrediction.getLocalTargetPath()));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name(namespace())
                .startupCommand(bash)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(4, 12))
                .workingDiskSpaceGb(375)
                .build();
    }

    @Override
    public ProtectOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return ProtectOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.PROTECT_EVIDENCE_TSV, metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), metadata.tumor().sampleName() + PROTECT_EVIDENCE_TSV)))
                .build();
    }

    @Override
    public ProtectOutput skippedOutput(final SomaticRunMetadata metadata) {
        return ProtectOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public ProtectOutput persistedOutput(final SomaticRunMetadata metadata) {
        return ProtectOutput.builder().status(PipelineStatus.PERSISTED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && arguments.refGenomeVersion().equals(RefGenomeVersion.V37) && !arguments.shallow();
    }
}
