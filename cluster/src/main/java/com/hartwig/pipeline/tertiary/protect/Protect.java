package com.hartwig.pipeline.tertiary.protect;

import static com.hartwig.pipeline.execution.vm.InputDownload.initialiseOptionalLocation;

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
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.chord.ChordOutput;
import com.hartwig.pipeline.tertiary.lilac.LilacOutput;
import com.hartwig.pipeline.tertiary.linx.LinxSomaticOutput;
import com.hartwig.pipeline.tertiary.linx.LinxSomaticOutputLocations;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutputLocations;
import com.hartwig.pipeline.tertiary.virus.VirusInterpreterOutput;

import org.jetbrains.annotations.NotNull;

@Namespace(Protect.NAMESPACE)
public class Protect implements Stage<ProtectOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "protect";
    public static final String PROTECT_EVIDENCE_TSV = ".protect.tsv";

    private final InputDownload purplePurity;
    private final InputDownload purpleQCFile;
    private final InputDownload purpleGeneCopyNumber;
    private final InputDownload purpleSomaticDriverCatalog;
    private final InputDownload purpleGermlineDriverCatalog;
    private final InputDownload purpleSomaticVariants;
    private final InputDownload purpleGermlineVariants;
    private final InputDownload linxFusions;
    private final InputDownload linxBreakends;
    private final InputDownload linxDriverCatalog;
    private final InputDownload virusAnnotations;
    private final InputDownload chordPrediction;
    private final InputDownload lilacQcCsv;
    private final InputDownload lilacResultsCsv;
    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public Protect(final PurpleOutput purpleOutput, final LinxSomaticOutput linxOutput, final VirusInterpreterOutput virusOutput,
            final ChordOutput chordOutput, final LilacOutput lilacOutput, final ResourceFiles resourceFiles,
            final PersistedDataset persistedDataset) {
        PurpleOutputLocations purpleOutputLocations = purpleOutput.outputLocations();
        this.purplePurity = new InputDownload(purpleOutputLocations.purity());
        this.purpleQCFile = new InputDownload(purpleOutputLocations.qcFile());
        this.purpleGeneCopyNumber = initialiseOptionalLocation(purpleOutputLocations.geneCopyNumber());
        this.purpleSomaticDriverCatalog = initialiseOptionalLocation(purpleOutputLocations.somaticDriverCatalog());
        this.purpleGermlineDriverCatalog = initialiseOptionalLocation(purpleOutputLocations.germlineDriverCatalog());
        this.purpleSomaticVariants = initialiseOptionalLocation(purpleOutputLocations.somaticVariants());
        this.purpleGermlineVariants = initialiseOptionalLocation(purpleOutputLocations.germlineVariants());
        LinxSomaticOutputLocations linxSomaticOutputLocations = linxOutput.linxOutputLocations();
        this.linxFusions = new InputDownload(linxSomaticOutputLocations.fusions());
        this.linxBreakends = new InputDownload(linxSomaticOutputLocations.breakends());
        this.linxDriverCatalog = new InputDownload(linxSomaticOutputLocations.driverCatalog());
        this.virusAnnotations = new InputDownload(virusOutput.virusAnnotations());
        this.chordPrediction = new InputDownload(chordOutput.predictions());
        this.lilacQcCsv = initialiseOptionalLocation(lilacOutput.qc());
        this.lilacResultsCsv = initialiseOptionalLocation(lilacOutput.result());
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(purplePurity,
                purpleQCFile,
                purpleGeneCopyNumber,
                purpleSomaticDriverCatalog,
                purpleGermlineDriverCatalog,
                purpleSomaticVariants,
                purpleGermlineVariants,
                linxFusions,
                linxBreakends,
                linxDriverCatalog,
                virusAnnotations,
                chordPrediction,
                lilacQcCsv,
                lilacResultsCsv);
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return List.of(new ProtectCommand(metadata.tumor().sampleName(),
                metadata.reference().sampleName(),
                metadata.tumor().primaryTumorDoids(),
                VmDirectories.OUTPUT,
                resourceFiles.actionabilityDir(),
                resourceFiles.version(),
                resourceFiles.driverGenePanel(),
                resourceFiles.doidJson(),
                purplePurity.getLocalTargetPath(),
                purpleQCFile.getLocalTargetPath(),
                purpleGeneCopyNumber.getLocalTargetPath(),
                purpleSomaticDriverCatalog.getLocalTargetPath(),
                purpleGermlineDriverCatalog.getLocalTargetPath(),
                purpleSomaticVariants.getLocalTargetPath(),
                purpleGermlineVariants.getLocalTargetPath(),
                linxFusions.getLocalTargetPath(),
                linxBreakends.getLocalTargetPath(),
                linxDriverCatalog.getLocalTargetPath(),
                virusAnnotations.getLocalTargetPath(),
                chordPrediction.getLocalTargetPath(),
                lilacResultsCsv.getLocalTargetPath(),
                lilacQcCsv.getLocalTargetPath()));
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
        final String evidenceTsv = evidenceTsv(metadata);
        return ProtectOutput.builder()
                .status(jobStatus)
                .maybeEvidence(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(evidenceTsv)))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.PROTECT_EVIDENCE,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), evidenceTsv)))
                .build();
    }

    @NotNull
    protected String evidenceTsv(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PROTECT_EVIDENCE_TSV;
    }

    @Override
    public ProtectOutput skippedOutput(final SomaticRunMetadata metadata) {
        return ProtectOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public ProtectOutput persistedOutput(final SomaticRunMetadata metadata) {
        String evidenceTsv = evidenceTsv(metadata);
        return ProtectOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeEvidence(persistedDataset.path(metadata.tumor().sampleName(), DataType.PROTECT_EVIDENCE)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), evidenceTsv))))
                .addDatatypes(new AddDatatype(DataType.PROTECT_EVIDENCE,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), evidenceTsv)))
                .build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow();
    }
}
