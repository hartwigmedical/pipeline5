package com.hartwig.pipeline.tertiary.teal;

import static com.hartwig.pipeline.tools.HmfTool.TEAL;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.hartwig.computeengine.execution.vm.Bash;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.java.JavaClassCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.DataType;
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
import com.hartwig.pipeline.tertiary.TertiaryStage;

@Namespace(TealBam.NAMESPACE)
public class TealBam extends TertiaryStage<TealBamOutput> {

    public static final String NAMESPACE = "teal_bam";
    private static final String TELBAM_APP_CLASS = "com.hartwig.hmftools.teal.TealPipelineTelbamApp";

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public TealBam(final AlignmentPair alignmentPair,
            final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        super(alignmentPair);
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public String namespace() { return NAMESPACE; }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {

        List<String> arguments = new ArrayList<>();

        addTumor(arguments, metadata);
        addReference(arguments, metadata);
        addCommonArguments(arguments);

        return formCommand(arguments);
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata)
    {
        List<String> arguments = new ArrayList<>();

        addTumor(arguments, metadata);
        addCommonArguments(arguments);

        return formCommand(arguments);
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata)
    {
        List<String> arguments = new ArrayList<>();

        addReference(arguments, metadata);
        addCommonArguments(arguments);

        return formCommand(arguments);
    }

    private List<BashCommand> formCommand(final List<String> arguments)
    {
        List<BashCommand> commands = new ArrayList<>();
        commands.add(new JavaClassCommand(
                TEAL.getToolName(),
                TEAL.runVersion(),
                TEAL.jar(),
                TELBAM_APP_CLASS,
                "30G",
                Collections.emptyList(),
                arguments));
        return commands;
    }

    private void addTumor(final List<String> arguments, final SomaticRunMetadata metadata) {
        arguments.add(String.format("-tumor %s", metadata.tumor().sampleName()));
        arguments.add(String.format("-tumor_bam %s", getTumorBamDownload().getLocalTargetPath()));
    }

    private void addReference(final List<String> arguments, final SomaticRunMetadata metadata) {
        arguments.add(String.format("-reference %s", metadata.reference().sampleName()));
        arguments.add(String.format("-reference_bam %s", getReferenceBamDownload().getLocalTargetPath()));
    }

    private void addCommonArguments(final List<String> arguments) {
        arguments.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(String.format("-threads %s", Bash.allCpus()));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.tealBam(startupScript, resultsDirectory);
    }

    @Override
    public TealBamOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {

        ImmutableTealBamOutput.Builder outputLocationsBuilder = TealBamOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE));

        metadata.maybeReference().ifPresent(reference -> {
            final String germlineSampleName = reference.sampleName();
            String germlineTelbam = telbam(germlineSampleName);

            outputLocationsBuilder.germlineTelbam(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(germlineTelbam)));
        });

        metadata.maybeTumor().ifPresent(tumor -> {
            final String tumorSampleName = tumor.sampleName();
            String somaticTelbam = telbam(tumorSampleName);

            outputLocationsBuilder
                    .somaticTelbam(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticTelbam)));
        });

        return outputLocationsBuilder.build();
    }

    @Override
    public TealBamOutput skippedOutput(final SomaticRunMetadata metadata) {
        return TealBamOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public TealBamOutput persistedOutput(final SomaticRunMetadata metadata) {

        ImmutableTealBamOutput.Builder outputLocationsBuilder = TealBamOutput.builder()
                .status(PipelineStatus.PERSISTED).addAllDatatypes(addDatatypes(metadata));

        metadata.maybeReference().ifPresent(reference -> {
            final String germlineSampleName = reference.sampleName();
            String germlineTelbam = telbam(germlineSampleName);
            outputLocationsBuilder
                    .germlineTelbam(persistedOrDefault(
                            germlineSampleName, metadata.set(), metadata.bucket(), DataType.TEAL_GERMLINE_TELBAM, germlineTelbam));
        });

        metadata.maybeTumor().ifPresent(tumor -> {
            final String tumorSampleName = tumor.sampleName();
            String somaticTelbam = telbam(tumorSampleName);
            outputLocationsBuilder
                    .somaticTelbam(persistedOrDefault(
                            tumorSampleName, metadata.set(), metadata.bucket(), DataType.TEAL_SOMATIC_TELBAM, somaticTelbam));
        });

        return outputLocationsBuilder.build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        List<AddDatatype> datatypes = new ArrayList<>();

        metadata.maybeReference().ifPresent(reference -> {
            final String germlineSampleName = reference.sampleName();
            String germlineTelbam = telbam(germlineSampleName);

            datatypes.add(new AddDatatype(DataType.TEAL_GERMLINE_TELBAM,
                    metadata.barcode(),
                    new ArchivePath(Folder.root(), namespace(), germlineTelbam)));
        });

        metadata.maybeTumor().ifPresent(tumor -> {
            final String tumorSampleName = tumor.sampleName();
            String somaticTelbam = telbam(tumorSampleName);

            datatypes.add(new AddDatatype(DataType.TEAL_SOMATIC_TELBAM,
                    metadata.barcode(),
                    new ArchivePath(Folder.root(), namespace(), somaticTelbam)));
        });

        return datatypes;
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.useTargetRegions();
    }

    private GoogleStorageLocation persistedOrDefault(final String sample, final String set, final String bucket,
            final DataType dataType, final String fileName) {
        return persistedDataset.path(sample, dataType)
                .orElse(GoogleStorageLocation.of(bucket, PersistedLocations.blobForSet(set, namespace(), fileName)));
    }

    private static String telbam(final String sample) {
        return sample + ".teal.telbam.bam";
    }
}
