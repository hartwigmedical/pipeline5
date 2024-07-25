package com.hartwig.pipeline.calling.structural;

import static com.hartwig.pipeline.calling.structural.SvCalling.ESVEE_GERMLINE_VCF;
import static com.hartwig.pipeline.calling.structural.SvCalling.ESVEE_SOMATIC_VCF;
import static com.hartwig.pipeline.calling.structural.SvCalling.ESVEE_UNFILTERED_VCF;

import java.util.List;

import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.structural.ImmutableEsveeOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.EntireOutputComponent;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.output.StartupScriptComponent;
import com.hartwig.pipeline.output.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.tertiary.TertiaryStage;

@Namespace(Esvee.NAMESPACE)
public class Esvee extends TertiaryStage<EsveeOutput> {
    public static final String NAMESPACE = "esvee";

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public Esvee(final AlignmentPair pair, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        super(pair);
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        String tumorSampleName = metadata.tumor().sampleName();
        String tumorBamPath = getTumorBamDownload().getLocalTargetPath();

        return new SvCalling(resourceFiles)
                .tumorSample(tumorSampleName, tumorBamPath)
                .apply(SubStageInputOutput.empty(metadata.tumor().sampleName()))
                .bash();
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        String referenceSampleName = metadata.reference().sampleName();
        String refBamPath = getReferenceBamDownload().getLocalTargetPath();

        return new SvCalling(resourceFiles)
                .referenceSample(referenceSampleName, refBamPath)
                .apply(SubStageInputOutput.empty(metadata.tumor().sampleName()))
                .bash();
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        String referenceSampleName = metadata.reference().sampleName();
        String tumorSampleName = metadata.tumor().sampleName();
        String refBamPath = getReferenceBamDownload().getLocalTargetPath();
        String tumorBamPath = getTumorBamDownload().getLocalTargetPath();

        return new SvCalling(resourceFiles)
                .tumorSample(tumorSampleName, tumorBamPath)
                .referenceSample(referenceSampleName, refBamPath)
                .apply(SubStageInputOutput.empty(metadata.tumor().sampleName()))
                .bash();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.esvee(bash, resultsDirectory);
    }

    private static String mainSampleName(SomaticRunMetadata metadata) {
        return (metadata.maybeTumor().isPresent()) ?
                metadata.tumor().sampleName() :
                metadata.reference().sampleName();
    }

    private static String formSampleOutputFilename(SomaticRunMetadata metadata, String filenameSuffix) {
        return String.format("%s.%s", mainSampleName(metadata), filenameSuffix);
    }

    private static GoogleStorageLocation formOutputLocation(final RuntimeBucket bucket, final ResultsDirectory resultsDirectory,
            final String filename) {
        return GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(filename));
    }

    @Override
    public EsveeOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {

        final ImmutableEsveeOutput.Builder builder = EsveeOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))

                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.root(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.root()))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory));


        String unfilteredVcf = formSampleOutputFilename(metadata, ESVEE_UNFILTERED_VCF);
        String somaticVcf = formSampleOutputFilename(metadata, ESVEE_SOMATIC_VCF);
        String germlineVcf = formSampleOutputFilename(metadata, ESVEE_GERMLINE_VCF);

        builder
                .maybeUnfilteredVcf(formOutputLocation(bucket, resultsDirectory, unfilteredVcf))
                .maybeUnfilteredVcfIndex(formOutputLocation(bucket, resultsDirectory, unfilteredVcf + FileTypes.TBI))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket, NAMESPACE, Folder.root(), unfilteredVcf, resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.UNFILTERED_STRUCTURAL_VARIANTS_ESVEE,
                        metadata.barcode(), new ArchivePath(Folder.root(), namespace(), unfilteredVcf)
                ));

        if(metadata.maybeTumor().isPresent()) {
            builder
                    .maybeSomaticVcf(formOutputLocation(bucket, resultsDirectory, somaticVcf))
                    .maybeSomaticVcfIndex(formOutputLocation(bucket, resultsDirectory, somaticVcf + FileTypes.TBI))
                    .addReportComponents(new ZippedVcfAndIndexComponent(bucket, NAMESPACE, Folder.root(), somaticVcf, resultsDirectory))
                    .addDatatypes(new AddDatatype(
                            DataType.SOMATIC_STRUCTURAL_VARIANTS_ESVEE,
                            metadata.barcode(), new ArchivePath(Folder.root(), namespace(), somaticVcf)
                    ));
        }

        if(metadata.maybeReference().isPresent()) {
            builder
                    .maybeGermlineVcf(formOutputLocation(bucket, resultsDirectory, germlineVcf))
                    .maybeGermlineVcfIndex(formOutputLocation(bucket, resultsDirectory, germlineVcf + FileTypes.TBI))
                    .addReportComponents(new ZippedVcfAndIndexComponent(bucket, NAMESPACE, Folder.root(), germlineVcf, resultsDirectory))
                    .addDatatypes(new AddDatatype(
                            DataType.GERMLINE_STRUCTURAL_VARIANTS_ESVEE,
                            metadata.barcode(), new ArchivePath(Folder.root(), namespace(), germlineVcf)
                    ));
        }

        return builder.build();
    }

    @Override
    public EsveeOutput skippedOutput(final SomaticRunMetadata metadata) {
        return EsveeOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    private GoogleStorageLocation formPersistedOutputLocation(final SomaticRunMetadata metadata, DataType dataType, String filenameSuffix) {

        return persistedDataset.path(mainSampleName(metadata), dataType)
                .orElse(GoogleStorageLocation.of(
                        metadata.bucket(),
                        PersistedLocations.blobForSet(
                                metadata.set(),
                                namespace(),
                                formSampleOutputFilename(metadata, filenameSuffix)
                        )
                ));
    }

    @Override
    public EsveeOutput persistedOutput(final SomaticRunMetadata metadata) {

        final ImmutableEsveeOutput.Builder builder = EsveeOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeUnfilteredVcf(formPersistedOutputLocation(
                        metadata, DataType.UNFILTERED_STRUCTURAL_VARIANTS_ESVEE, ESVEE_UNFILTERED_VCF))
                .maybeUnfilteredVcfIndex(formPersistedOutputLocation(
                        metadata, DataType.UNFILTERED_STRUCTURAL_VARIANTS_ESVEE, ESVEE_UNFILTERED_VCF + FileTypes.TBI));

        if(metadata.maybeTumor().isPresent()) {
            builder
                    .maybeSomaticVcf(formPersistedOutputLocation(
                            metadata, DataType.SOMATIC_STRUCTURAL_VARIANTS_ESVEE, ESVEE_SOMATIC_VCF))
                    .maybeSomaticVcfIndex(formPersistedOutputLocation(
                            metadata, DataType.SOMATIC_STRUCTURAL_VARIANTS_ESVEE, ESVEE_SOMATIC_VCF + FileTypes.TBI));
        }

        if(metadata.maybeReference().isPresent()) {
            builder
                    .maybeGermlineVcf(formPersistedOutputLocation(
                            metadata, DataType.GERMLINE_STRUCTURAL_VARIANTS_ESVEE, ESVEE_GERMLINE_VCF))
                    .maybeGermlineVcfIndex(formPersistedOutputLocation(
                            metadata, DataType.GERMLINE_STRUCTURAL_VARIANTS_ESVEE, ESVEE_GERMLINE_VCF + FileTypes.TBI));
        }

        return builder.build();
    }
}
