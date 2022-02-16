package com.hartwig.pipeline.tertiary.peach;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.python.Python3Command;
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
import com.hartwig.pipeline.tertiary.purple.PurpleOutputLocations;
import com.hartwig.pipeline.tools.Versions;

import org.jetbrains.annotations.NotNull;

public class Peach implements Stage<PeachOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "peach";
    private static final String PEACH_CALLS_TSV = ".peach.calls.tsv";
    public static final String PEACH_GENOTYPE_TSV = ".peach.genotype.tsv";

    private final InputDownload purpleGermlineVcfDownload;
    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public Peach(final PurpleOutput purpleOutput, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        purpleGermlineVcfDownload = new InputDownload(purpleOutput.maybeOutputLocations()
                .map(PurpleOutputLocations::germlineVcf)
                .orElse(GoogleStorageLocation.empty()));
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(purpleGermlineVcfDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return List.of(new Python3Command("peach",
                Versions.PEACH,
                "src/main.py",
                List.of("--vcf",
                        purpleGermlineVcfDownload.getLocalTargetPath(),
                        "--sample_t_id",
                        metadata.tumor().sampleName(),
                        "--sample_r_id",
                        metadata.reference().sampleName(),
                        "--tool_version",
                        Versions.PEACH,
                        "--outputdir",
                        VmDirectories.OUTPUT,
                        "--panel",
                        resourceFiles.peachFilterBed())));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .name(NAMESPACE)
                .startupCommand(bash)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(2, 4))
                .workingDiskSpaceGb(375)
                .build();
    }

    @Override
    public PeachOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        final String genotypeTsv = genotypeTsv(metadata);
        return PeachOutput.builder()
                .status(jobStatus)
                .maybeGenotypeTsv(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(genotypeTsv)))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.PEACH_CALLS,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), metadata.tumor().sampleName() + PEACH_CALLS_TSV)),
                        new AddDatatype(DataType.PEACH_GENOTYPE,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), metadata.tumor().sampleName() + PEACH_GENOTYPE_TSV)))
                .build();
    }

    @NotNull
    protected String genotypeTsv(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + ".peach.genotype.tsv";
    }

    @Override
    public PeachOutput skippedOutput(final SomaticRunMetadata metadata) {
        return PeachOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public PeachOutput persistedOutput(final SomaticRunMetadata metadata) {
        String genotypeTsv = genotypeTsv(metadata);
        return PeachOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeGenotypeTsv(persistedDataset.path(metadata.tumor().sampleName(), DataType.PEACH_GENOTYPE)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), genotypeTsv))))
                .addDatatypes(new AddDatatype(DataType.PEACH_GENOTYPE,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), genotypeTsv)))
                .build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return !arguments.shallow() && arguments.runTertiary();
    }
}
