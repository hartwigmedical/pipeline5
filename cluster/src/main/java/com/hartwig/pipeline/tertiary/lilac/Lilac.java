package com.hartwig.pipeline.tertiary.lilac;

import static java.lang.String.format;

import static com.hartwig.computeengine.execution.vm.command.InputDownloadCommand.initialiseOptionalLocation;
import static com.hartwig.pipeline.tools.HmfTool.LILAC;

import java.util.ArrayList;
import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.computeengine.execution.vm.Bash;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.execution.vm.command.java.JavaJarCommand;
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
import com.hartwig.pipeline.tertiary.purple.PurpleOutputLocations;

@Namespace(Lilac.NAMESPACE)
public class Lilac implements Stage<LilacOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "lilac";

    private final ResourceFiles resourceFiles;
    private final LilacBamSliceOutput slicedOutput;
    private final InputDownloadCommand purpleGeneCopyNumber;
    private final InputDownloadCommand purpleSomaticVariants;
    private final InputDownloadCommand slicedReference;
    private final InputDownloadCommand slicedTumor;
    private final PersistedDataset persistedDataset;

    public Lilac(final LilacBamSliceOutput slicedOutput, final ResourceFiles resourceFiles, final PurpleOutput purpleOutput,
            final PersistedDataset persistedDataset) {
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
        PurpleOutputLocations purpleOutputLocations = purpleOutput.outputLocations();
        this.purpleGeneCopyNumber = initialiseOptionalLocation(purpleOutputLocations.geneCopyNumber());
        this.purpleSomaticVariants = initialiseOptionalLocation(purpleOutputLocations.somaticVariants());
        this.slicedOutput = slicedOutput;
        this.slicedReference = initialiseOptionalLocation(slicedOutput.reference());
        this.slicedTumor = initialiseOptionalLocation(slicedOutput.tumor());
    }

    @Override
    public List<BashCommand> inputs() {
        List<BashCommand> result = new ArrayList<>();
        result.add(purpleGeneCopyNumber);
        result.add(purpleSomaticVariants);
        result.add(slicedReference);
        result.add(slicedTumor);
        result.add(initialiseOptionalLocation(slicedOutput.tumorIndex()));
        result.add(initialiseOptionalLocation(slicedOutput.referenceIndex()));
        return result;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {

        List<String> arguments = Lists.newArrayList();
        arguments.add(format("-sample %s", metadata.tumor().sampleName()));
        arguments.add(format("-tumor_bam %s", slicedTumor.getLocalTargetPath()));
        arguments.addAll(commonArguments());
        return List.of(formCommand(arguments));
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {

        List<String> arguments = Lists.newArrayList();
        arguments.add(format("-sample %s", metadata.reference().sampleName()));
        arguments.add(format("-reference_bam %s", slicedReference.getLocalTargetPath()));
        arguments.addAll(commonArguments());
        return List.of(formCommand(arguments));
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {

        List<String> arguments = Lists.newArrayList();
        arguments.add(format("-sample %s", metadata.tumor().sampleName()));
        arguments.add(format("-reference_bam %s", slicedReference.getLocalTargetPath()));
        arguments.add(format("-tumor_bam %s", slicedTumor.getLocalTargetPath()));
        arguments.addAll(commonArguments());
        arguments.add(format("-gene_copy_number %s", purpleGeneCopyNumber.getLocalTargetPath()));
        arguments.add(format("-somatic_vcf %s", purpleSomaticVariants.getLocalTargetPath()));

        return List.of(formCommand(arguments));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.lilac(bash, resultsDirectory);
    }

    @Override
    public LilacOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        String lilacOutput = lilacOutput(metadata.sampleName());
        String lilacQc = lilacQcMetrics(metadata.sampleName());
        return LilacOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .qc(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(lilacQc)))
                .result(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(lilacOutput)))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public LilacOutput skippedOutput(final SomaticRunMetadata metadata) {
        return LilacOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public LilacOutput persistedOutput(final SomaticRunMetadata metadata) {
        String lilacOutput = lilacOutput(metadata.sampleName());
        String lilacQc = lilacQcMetrics(metadata.sampleName());
        return LilacOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .qc(persistedDataset.path(metadata.tumor().sampleName(), DataType.LILAC_QC_METRICS)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), lilacQc))))
                .result(persistedDataset.path(metadata.tumor().sampleName(), DataType.LILAC_OUTPUT)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), lilacOutput))))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        return List.of(new AddDatatype(DataType.LILAC_OUTPUT,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), lilacOutput(metadata.sampleName()))),
                new AddDatatype(DataType.LILAC_QC_METRICS,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), lilacQcMetrics(metadata.sampleName()))));
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow();
    }

    private List<String> commonArguments() {

        List<String> arguments = Lists.newArrayList();
        arguments.add(format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(format("-ref_genome_version %s", resourceFiles.version()));
        arguments.add(format("-resource_dir %s", resourceFiles.lilacResources()));
        arguments.add(format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(format("-threads %s", Bash.allCpus()));

        return arguments;
    }

    private JavaJarCommand formCommand(final List<String> arguments) {
        return JavaCommandFactory.javaJarCommand(LILAC, arguments);
    }

    private String lilacOutput(final String sampleName) {
        return sampleName + ".lilac.tsv";
    }

    private String lilacQcMetrics(final String sampleName) {
        return sampleName + ".lilac.qc.tsv";
    }
}
