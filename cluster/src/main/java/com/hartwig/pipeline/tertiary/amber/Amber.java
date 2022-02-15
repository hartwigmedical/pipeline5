package com.hartwig.pipeline.tertiary.amber;

import java.io.File;
import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.GermlineOnlyCommand;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.CopyResourceToOutput;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;
import com.hartwig.pipeline.tertiary.TumorNormalCommand;
import com.hartwig.pipeline.tertiary.TumorOnlyCommand;
import com.hartwig.pipeline.tools.Versions;

public class Amber extends TertiaryStage<AmberOutput> {

    public static final String NAMESPACE = "amber";
    private static final String JAR = "amber.jar";
    private static final String MAX_HEAP = "32G";
    private static final String MAIN_CLASS = "com.hartwig.hmftools.amber.AmberApplication";

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public Amber(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        super(alignmentPair);
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        return List.of(new JavaClassCommand("amber",
                Versions.AMBER,
                JAR,
                MAIN_CLASS,
                MAX_HEAP,
                "-tumor",
                metadata.tumor().sampleName(),
                "-tumor_bam",
                getTumorBamDownload().getLocalTargetPath(),
                "-output_dir",
                VmDirectories.OUTPUT,
                "-threads",
                Bash.allCpus(),
                "-ref_genome",
                resourceFiles.refGenomeFile(),
                "-loci",
                resourceFiles.amberHeterozygousLoci()));
    }

    @Override
    public List<BashCommand> germlineCommands(final SomaticRunMetadata metadata) {
        return List.of(new JavaClassCommand("amber",
                Versions.AMBER,
                JAR,
                MAIN_CLASS,
                MAX_HEAP,
                "-reference",
                metadata.reference().sampleName(),
                "-reference_bam",
                getReferenceBamDownload().getLocalTargetPath(),
                "-output_dir",
                VmDirectories.OUTPUT,
                "-threads",
                Bash.allCpus(),
                "-ref_genome",
                resourceFiles.refGenomeFile(),
                "-loci",
                resourceFiles.amberHeterozygousLoci()));
    }

    @Override
    public List<BashCommand> somaticCommands(final SomaticRunMetadata metadata) {
        return List.of(new JavaClassCommand("amber",
                Versions.AMBER,
                JAR,
                MAIN_CLASS,
                MAX_HEAP,
                "-reference",
                metadata.reference().sampleName(),
                "-reference_bam",
                getReferenceBamDownload().getLocalTargetPath(),
                "-tumor",
                metadata.tumor().sampleName(),
                "-tumor_bam",
                getTumorBamDownload().getLocalTargetPath(),
                "-output_dir",
                VmDirectories.OUTPUT,
                "-threads",
                Bash.allCpus(),
                "-ref_genome",
                resourceFiles.refGenomeFile(),
                "-loci",
                resourceFiles.amberHeterozygousLoci()));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.amber(bash, resultsDirectory);
    }

    @Override
    public AmberOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return AmberOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeOutputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.AMBER,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), String.format("%s.amber.baf.tsv", metadata.sampleName()))),
                        new AddDatatype(DataType.AMBER_SNPCHECK,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), new File(resourceFiles.amberSnpcheck()).getName())))
                .build();
    }

    @Override
    public AmberOutput skippedOutput(final SomaticRunMetadata metadata) {
        return AmberOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public AmberOutput persistedOutput(final SomaticRunMetadata metadata) {
        return AmberOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeOutputDirectory(persistedDataset.path(metadata.tumor().sampleName(), DataType.AMBER)
                        .map(GoogleStorageLocation::asDirectory)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.pathForSet(metadata.set(), namespace()),
                                true)))
                .build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary();
    }
}
