package com.hartwig.pipeline.tertiary.lilac;

import static com.hartwig.pipeline.execution.vm.InputDownload.initialiseOptionalLocation;

import java.util.ArrayList;
import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
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
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutputLocations;
import com.hartwig.pipeline.tools.Versions;

@Namespace(Lilac.NAMESPACE)
public class Lilac implements Stage<LilacOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "lilac";

    private final ResourceFiles resourceFiles;
    private final LilacBamSliceOutput slicedOutput;
    private final InputDownload purpleGeneCopyNumber;
    private final InputDownload purpleSomaticVariants;
    private final InputDownload slicedReference;
    private final InputDownload slicedTumor;
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
    public String namespace() {
        return NAMESPACE;
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
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        List<String> arguments = Lists.newArrayList();
        arguments.addAll(commonArguments(metadata.tumor().sampleName(), slicedReference.getLocalTargetPath()));
        arguments.add(String.format("-tumor_bam %s", slicedTumor.getLocalTargetPath()));
        arguments.add(String.format("-gene_copy_number %s", purpleGeneCopyNumber.getLocalTargetPath()));
        arguments.add(String.format("-somatic_vcf %s", purpleSomaticVariants.getLocalTargetPath()));

        return List.of(formCommand(arguments));
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        return List.of(formCommand(commonArguments(metadata.reference().sampleName(), slicedReference.getLocalTargetPath())));
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        return List.of(formCommand(commonArguments(metadata.tumor().sampleName(), slicedTumor.getLocalTargetPath())));
    }

    private List<String> commonArguments(final String sampleName, final String bamFile) {
        List<String> arguments = Lists.newArrayList();

        arguments.add(String.format("-sample %s", sampleName));
        arguments.add(String.format("-reference_bam %s", bamFile));
        arguments.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(String.format("-ref_genome_version %s", resourceFiles.version()));
        arguments.add(String.format("-resource_dir %s", resourceFiles.lilacResources()));
        arguments.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(String.format("-threads %s", Bash.allCpus()));

        return arguments;
    }

    private JavaJarCommand formCommand(final List<String> arguments) {
        return new JavaJarCommand("lilac", Versions.LILAC, "lilac.jar", "15G", arguments);
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("lilac")
                .startupCommand(bash)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(8, 16))
                .namespacedResults(resultsDirectory)
                .build();
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

    private String lilacOutput(final String sampleName) {
        return sampleName + ".lilac.csv";
    }

    private String lilacQcMetrics(final String sampleName) {
        return sampleName + ".lilac.qc.csv";
    }
}
