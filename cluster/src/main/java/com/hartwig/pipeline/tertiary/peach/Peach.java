package com.hartwig.pipeline.tertiary.peach;

import static java.lang.String.format;

import static com.hartwig.computeengine.execution.vm.command.InputDownloadCommand.initialiseOptionalLocation;
import static com.hartwig.pipeline.tools.HmfTool.PEACH;

import java.util.Collections;
import java.util.List;

import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.JavaCommandFactory;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.EntireOutputComponent;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

import org.jetbrains.annotations.NotNull;

@Namespace(Peach.NAMESPACE)
public class Peach implements Stage<PeachOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "peach";
    public static final String PEACH_EVENTS_TSV = ".peach.events.tsv";
    public static final String PEACH_EVENTS_PER_GENE_TSV = ".peach.gene.events.tsv";
    public static final String PEACH_ALL_HAPLOTYPES_TSV = ".peach.haplotypes.all.tsv";
    public static final String PEACH_GENOTYPE_TSV = ".peach.haplotypes.best.tsv";
    public static final String PEACH_QC_TSV = ".peach.qc.tsv";
    private final InputDownloadCommand purpleGermlineVariantsDownload;
    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public Peach(final PurpleOutput purpleOutput, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        purpleGermlineVariantsDownload = initialiseOptionalLocation(purpleOutput.outputLocations().germlineVariants());
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(purpleGermlineVariantsDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        return peachCommands(metadata.reference().sampleName());
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        return peachCommands(metadata.reference().sampleName());
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.peach(bash, resultsDirectory);
    }

    @Override
    public PeachOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return PeachOutput.builder()
                .status(jobStatus)
                .maybeGenotypes(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(genotypeTsv(metadata.reference().sampleName()))))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public PeachOutput skippedOutput(final SomaticRunMetadata metadata) {
        return PeachOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public PeachOutput persistedOutput(final SomaticRunMetadata metadata) {
        String genotypeTsv = genotypeTsv(metadata.reference().sampleName());
        return PeachOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeGenotypes(persistedDataset.path(metadata.sampleName(), DataType.PEACH_GENOTYPE)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), genotypeTsv))))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        String sampleName = metadata.reference().sampleName();
        return List.of(new AddDatatype(DataType.PEACH_CALLS,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), sampleName + PEACH_EVENTS_TSV)),
                new AddDatatype(DataType.PEACH_CALLS_PER_GENE,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), sampleName + PEACH_EVENTS_PER_GENE_TSV)),
                new AddDatatype(DataType.PEACH_ALL_HAPLOTYPES,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), sampleName + PEACH_ALL_HAPLOTYPES_TSV)),
                new AddDatatype(DataType.PEACH_GENOTYPE,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), genotypeTsv(sampleName))),
                new AddDatatype(DataType.PEACH_QC,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), sampleName + PEACH_QC_TSV)));
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return !arguments.shallow() && arguments.runTertiary();
    }

    public List<BashCommand> peachCommands(final String referenceId) {
        List<String> arguments = List.of(
                format("-vcf_file %s", purpleGermlineVariantsDownload.getLocalTargetPath()),
                format("-sample_name %s", referenceId),
                format("-haplotypes_file %s", resourceFiles.peachHaplotypes()),
                format("-function_file %s", resourceFiles.peachHaplotypeFunctions()),
                format("-drugs_file %s", resourceFiles.peachDrugs()),
                format("-output_dir %s", VmDirectories.OUTPUT)
        );
        return Collections.singletonList(JavaCommandFactory.javaJarCommand(PEACH, arguments));
    }

    @NotNull
    protected String genotypeTsv(final String sampleName) {
        return sampleName + PEACH_GENOTYPE_TSV;
    }
}
