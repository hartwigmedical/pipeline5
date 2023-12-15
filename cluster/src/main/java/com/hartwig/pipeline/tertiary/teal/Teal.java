package com.hartwig.pipeline.tertiary.teal;

import static com.hartwig.computeengine.execution.vm.VirtualMachinePerformanceProfile.custom;
import static com.hartwig.pipeline.tools.HmfTool.TEAL;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.execution.vm.Bash;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.JavaCommandFactory;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
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
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutputLocations;

@Namespace(Teal.NAMESPACE)
public class Teal extends TertiaryStage<TealOutput> {

    public static final String NAMESPACE = "teal";

    private final InputDownloadCommand purpleOutputDirDownload;
    private final InputDownloadCommand cobaltOutputDirDownload;

    private final InputDownloadCommand referenceBamMetricsDownload;

    private final InputDownloadCommand tumorBamMetricsDownload;

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public Teal(final AlignmentPair alignmentPair,
            final PurpleOutput purpleOutput,
            final CobaltOutput cobaltOutput,
            final BamMetricsOutput referenceBamMetricsOutput,
            final BamMetricsOutput tumorBamMetricsOutput,
            final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        super(alignmentPair);
        PurpleOutputLocations purpleOutputLocations = purpleOutput.outputLocations();
        purpleOutputDirDownload = new InputDownloadCommand(purpleOutputLocations.outputDirectory());
        cobaltOutputDirDownload = new InputDownloadCommand(cobaltOutput.outputDirectory());
        referenceBamMetricsDownload = new InputDownloadCommand(referenceBamMetricsOutput.metricsOutputFile());
        tumorBamMetricsDownload = new InputDownloadCommand(tumorBamMetricsOutput.metricsOutputFile());
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> inputs() {

        // add all download of the super class
        List<BashCommand> commands = new ArrayList<>(super.inputs());
        commands.add(purpleOutputDirDownload);
        commands.add(cobaltOutputDirDownload);
        commands.add(referenceBamMetricsDownload);
        commands.add(tumorBamMetricsDownload);
        return ImmutableList.copyOf(commands);
    }

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
        commands.add(JavaCommandFactory.javaJarCommand(TEAL, arguments));
        return commands;
    }

    private void addTumor(final List<String> arguments, final SomaticRunMetadata metadata) {
        arguments.add(String.format("-tumor %s", metadata.tumor().sampleName()));
        arguments.add(String.format("-tumor_bam %s", getTumorBamDownload().getLocalTargetPath()));
        arguments.add(String.format("-tumor_wgs_metrics %s", tumorBamMetricsDownload.getLocalTargetPath()));
    }

    private void addReference(final List<String> arguments, final SomaticRunMetadata metadata) {
        arguments.add(String.format("-reference %s", metadata.reference().sampleName()));
        arguments.add(String.format("-reference_bam %s", getReferenceBamDownload().getLocalTargetPath()));
        arguments.add(String.format("-reference_wgs_metrics %s", referenceBamMetricsDownload.getLocalTargetPath()));
    }

    private void addCommonArguments(final List<String> arguments) {
        arguments.add(String.format("-purple %s", purpleOutputDirDownload.getLocalTargetPath()));
        arguments.add(String.format("-cobalt %s", cobaltOutputDirDownload.getLocalTargetPath()));
        arguments.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(String.format("-threads %s", Bash.allCpus()));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .imageFamily(IMAGE_FAMILY)
                .name("teal")
                .startupCommand(startupScript)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(TEAL.getCpus(), TEAL.getMemoryGb()))
                .build();
    }

    @Override
    public TealOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {

        ImmutableTealOutputLocations.Builder outputLocationsBuilder = TealOutputLocations.builder();
                //.outputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true));

        metadata.maybeTumor().ifPresent(tumor -> {
            final String tumorSampleName = tumor.sampleName();
            String somaticTellength = tellength(tumorSampleName);
            String somaticTelbam = telbam(tumorSampleName);
            String somaticBreakend = breakend(tumorSampleName);

            outputLocationsBuilder.somaticTellength(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticTellength)))
                    .somaticTelbam(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticTelbam)))
                    .somaticBreakend(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticBreakend)));
        });

        metadata.maybeReference().ifPresent(reference -> {
            final String germlineSampleName = reference.sampleName();
            String germlineTellength = tellength(germlineSampleName);
            String germlineTelbam = telbam(germlineSampleName);

            outputLocationsBuilder.germlineTellength(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(germlineTellength)))
                    .germlineTelbam(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(germlineTelbam)));
        });

        return TealOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .maybeOutputLocations(outputLocationsBuilder.build())
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public TealOutput skippedOutput(final SomaticRunMetadata metadata) {
        return TealOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public TealOutput persistedOutput(final SomaticRunMetadata metadata) {

        final String tumorSampleName = metadata.tumor().sampleName();
        String somaticTellength = tellength(tumorSampleName);
        String somaticTelbam = telbam(tumorSampleName);
        String somaticBreakend = breakend(tumorSampleName);
        final String germlineSampleName = metadata.reference().sampleName();
        String germlineTellength = tellength(germlineSampleName);
        String germlineTelbam = telbam(germlineSampleName);

        GoogleStorageLocation somaticTellengthLocation = persistedOrDefault(metadata.sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.TEAL_SOMATIC_TELLENGTH,
                somaticTellength);
        GoogleStorageLocation somaticTelbamLocation = persistedOrDefault(metadata.sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.TEAL_SOMATIC_TELBAM,
                somaticTelbam);
        GoogleStorageLocation somaticBreakendLocation = persistedOrDefault(metadata.sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.TEAL_SOMATIC_BREAKEND,
                somaticBreakend);
        GoogleStorageLocation germlineTellengthLocation = persistedOrDefault(metadata.sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.TEAL_GERMLINE_TELLENGTH,
                germlineTellength);
        GoogleStorageLocation germlineTelbamLocation = persistedOrDefault(metadata.sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.TEAL_GERMLINE_TELBAM,
                germlineTelbam);

        ImmutableTealOutputLocations.Builder outputLocationsBuilder = TealOutputLocations.builder()
                .somaticTellength(somaticTellengthLocation)
                .somaticTelbam(somaticTelbamLocation)
                .somaticBreakend(somaticBreakendLocation)
                .germlineTellength(germlineTellengthLocation)
                .germlineTelbam(germlineTelbamLocation);

        return TealOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .addAllDatatypes(addDatatypes(metadata))
                .maybeOutputLocations(outputLocationsBuilder.build())
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        List<AddDatatype> datatypes = new ArrayList<>();

        metadata.maybeTumor().ifPresent(tumor -> {
            final String tumorSampleName = tumor.sampleName();
            String somaticTellength = tellength(tumorSampleName);
            String somaticTelbam = telbam(tumorSampleName);
            String somaticBreakend = breakend(tumorSampleName);

            datatypes.add(new AddDatatype(DataType.TEAL_SOMATIC_TELLENGTH,
                            metadata.barcode(),
                            new ArchivePath(Folder.root(), namespace(), somaticTellength)));
            datatypes.add(new AddDatatype(DataType.TEAL_SOMATIC_TELBAM,
                    metadata.barcode(),
                    new ArchivePath(Folder.root(), namespace(), somaticTelbam)));
            datatypes.add(new AddDatatype(DataType.TEAL_SOMATIC_BREAKEND,
                    metadata.barcode(),
                    new ArchivePath(Folder.root(), namespace(), somaticBreakend)));
        });

        metadata.maybeReference().ifPresent(reference -> {
            final String germlineSampleName = reference.sampleName();
            String germlineTellength = tellength(germlineSampleName);
            String germlineTelbam = telbam(germlineSampleName);

            datatypes.add(new AddDatatype(DataType.TEAL_GERMLINE_TELLENGTH,
                    metadata.barcode(),
                    new ArchivePath(Folder.root(), namespace(), germlineTellength)));
            datatypes.add(new AddDatatype(DataType.TEAL_GERMLINE_TELBAM,
                    metadata.barcode(),
                    new ArchivePath(Folder.root(), namespace(), germlineTelbam)));
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

    private static String tellength(final String sample) {
        return sample + ".teal.tellength.tsv";
    }

    private static String telbam(final String sample) {
        return sample + ".teal.telbam.bam";
    }

    private static String breakend(final String sample) {
        return sample + ".teal.breakend.tsv.gz";
    }
}
