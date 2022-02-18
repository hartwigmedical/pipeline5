package com.hartwig.pipeline.tertiary.protect;

import java.util.List;
import java.util.function.Function;

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
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.chord.ChordOutput;
import com.hartwig.pipeline.tertiary.linx.LinxOutput;
import com.hartwig.pipeline.tertiary.linx.LinxOutputLocations;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutputLocations;
import com.hartwig.pipeline.tertiary.virus.VirusOutput;

import org.jetbrains.annotations.NotNull;

public class Protect implements Stage<ProtectOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "protect";
    public static final String PROTECT_EVIDENCE_TSV = ".protect.tsv";

    private final InputDownload purplePurity;
    private final InputDownload purpleQCFile;
    private final InputDownload purpleGeneCopyNumberTsv;
    private final InputDownload purpleSomaticDriverCatalog;
    private final InputDownload purpleGermlineDriverCatalog;
    private final InputDownload purpleSomaticVariants;
    private final InputDownload purpleGermlineVariants;
    private final InputDownload linxFusionTsv;
    private final InputDownload linxBreakendTsv;
    private final InputDownload linxDriverCatalogTsv;
    private final InputDownload annotatedVirusTsv;
    private final InputDownload chordPrediction;
    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public Protect(final PurpleOutput purpleOutput, final LinxOutput linxOutput, final VirusOutput virusOutput,
            final ChordOutput chordOutput, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        this.purplePurity = new InputDownload(purpleOrEmpty(purpleOutput, PurpleOutputLocations::purityTsv));
        this.purpleQCFile = new InputDownload(purpleOrEmpty(purpleOutput, PurpleOutputLocations::qcFile));
        this.purpleGeneCopyNumberTsv = new InputDownload(purpleOrEmpty(purpleOutput, PurpleOutputLocations::geneCopyNumberTsv));
        this.purpleSomaticDriverCatalog = new InputDownload(purpleOrEmpty(purpleOutput, PurpleOutputLocations::somaticDriverCatalog));
        this.purpleGermlineDriverCatalog = new InputDownload(purpleOrEmpty(purpleOutput, PurpleOutputLocations::germlineDriverCatalog));
        this.purpleSomaticVariants = new InputDownload(purpleOrEmpty(purpleOutput, PurpleOutputLocations::somaticVcf));
        this.purpleGermlineVariants = new InputDownload(purpleOrEmpty(purpleOutput, PurpleOutputLocations::germlineVcf));
        this.linxFusionTsv = new InputDownload(linxOrEmpty(linxOutput, LinxOutputLocations::fusions));
        this.linxBreakendTsv = new InputDownload(linxOrEmpty(linxOutput, LinxOutputLocations::breakends));
        this.linxDriverCatalogTsv = new InputDownload(linxOrEmpty(linxOutput, LinxOutputLocations::driverCatalog));
        this.annotatedVirusTsv = new InputDownload(virusOutput.maybeAnnotatedVirusFile().orElse(GoogleStorageLocation.empty()));
        this.chordPrediction = new InputDownload(chordOutput.maybePredictions().orElse(GoogleStorageLocation.empty()));
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    private GoogleStorageLocation purpleOrEmpty(final PurpleOutput purpleOutput,
            final Function<PurpleOutputLocations, GoogleStorageLocation> extractor) {
        return purpleOutput.maybeOutputLocations().map(extractor).orElse(GoogleStorageLocation.empty());
    }

    private GoogleStorageLocation linxOrEmpty(final LinxOutput linxOutput,
            final Function<LinxOutputLocations, GoogleStorageLocation> extractor) {
        return linxOutput.maybeLinxOutputLocations().map(extractor).orElse(GoogleStorageLocation.empty());
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(purplePurity,
                purpleQCFile,
                purpleGeneCopyNumberTsv,
                purpleSomaticDriverCatalog,
                purpleGermlineDriverCatalog,
                purpleSomaticVariants,
                purpleGermlineVariants,
                linxFusionTsv,
                linxBreakendTsv,
                linxDriverCatalogTsv,
                annotatedVirusTsv,
                chordPrediction);
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return List.of(new ProtectCommand(metadata.tumor().sampleName(),
                metadata.reference().sampleName(),
                metadata.tumor().primaryTumorDoids(),
                VmDirectories.OUTPUT,
                resourceFiles.actionabilityDir(),
                resourceFiles.version(),
                resourceFiles.doidJson(),
                purplePurity.getLocalTargetPath(),
                purpleQCFile.getLocalTargetPath(),
                purpleGeneCopyNumberTsv.getLocalTargetPath(),
                purpleSomaticDriverCatalog.getLocalTargetPath(),
                purpleGermlineDriverCatalog.getLocalTargetPath(),
                purpleSomaticVariants.getLocalTargetPath(),
                purpleGermlineVariants.getLocalTargetPath(),
                linxFusionTsv.getLocalTargetPath(),
                linxBreakendTsv.getLocalTargetPath(),
                linxDriverCatalogTsv.getLocalTargetPath(),
                annotatedVirusTsv.getLocalTargetPath(),
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
        final String evidenceTsv = evidenceTsv(metadata);
        return ProtectOutput.builder()
                .status(jobStatus)
                .maybeEvidenceTsv(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(evidenceTsv)))
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
        return ProtectOutput.builder().status(PipelineStatus.SKIPPED).maybeEvidenceTsv(GoogleStorageLocation.empty()).build();
    }

    @Override
    public ProtectOutput persistedOutput(final SomaticRunMetadata metadata) {
        String evidenceTsv = evidenceTsv(metadata);
        return ProtectOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeEvidenceTsv(persistedDataset.path(metadata.tumor().sampleName(), DataType.PROTECT_EVIDENCE)
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
