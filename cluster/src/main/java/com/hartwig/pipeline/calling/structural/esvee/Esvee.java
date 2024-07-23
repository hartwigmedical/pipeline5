package com.hartwig.pipeline.calling.structural.esvee;

import static java.lang.String.format;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.unix.ExportPathCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.command.BwaCommand;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.calling.structural.gridss.GridssAnnotation;
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
    private String unfilteredVcf;

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
        SvCalling svCalling = new SvCalling(resourceFiles).tumorSample(tumorSampleName,
                tumorBamPath);
        return esveeCommands(svCalling, tumorSampleName);
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        String referenceSampleName = metadata.reference().sampleName();
        String refBamPath = getReferenceBamDownload().getLocalTargetPath();
        SvCalling svCalling = new SvCalling(resourceFiles).referenceSample(
                referenceSampleName,
                refBamPath);
        return esveeCommands(svCalling, referenceSampleName);
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        String referenceSampleName = metadata.reference().sampleName();
        String tumorSampleName = metadata.tumor().sampleName();
        String refBamPath = getReferenceBamDownload().getLocalTargetPath();
        String tumorBamPath = getTumorBamDownload().getLocalTargetPath();
        return esveeCommands(new SvCalling(resourceFiles).tumorSample(
                tumorSampleName,
                tumorBamPath).referenceSample(referenceSampleName, refBamPath), tumorSampleName);
    }

    private List<BashCommand> esveeCommands(final SvCalling svCalling, final String sampleName) {
        SubStageInputOutput unfilteredVcfOutput = svCalling.apply(SubStageInputOutput.empty(sampleName));
        unfilteredVcf = unfilteredVcfOutput.outputFile().path();

        return unfilteredVcfOutput.bash();
    }

    private static String basename(final String filename) {
        return new File(filename).getName();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.gridds(bash, resultsDirectory);
    }

    @Override
    public EsveeOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return EsveeOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeUnfilteredVcfLocation(resultLocation(bucket, resultsDirectory, unfilteredVcf))
                .maybeUnfilteredVcfIndexLocation(resultLocation(bucket, resultsDirectory, unfilteredVcf + ".tbi"))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.root(),
                        basename(unfilteredVcf),
                        basename(unfilteredVcf),
                        resultsDirectory))
                .addReportComponents(new EntireOutputComponent(bucket,
                        Folder.root(),
                        NAMESPACE,
                        resultsDirectory,
                        s -> !s.contains("working") || s.endsWith("bam.sv.bam") || s.endsWith("bam.sv.bam.bai")))
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.root(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.root()))
                .addDatatypes(new AddDatatype(DataType.STRUCTURAL_VARIANTS_ESVEE,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), basename(unfilteredVcf))))
                .build();
    }

    @Override
    public EsveeOutput skippedOutput(final SomaticRunMetadata metadata) {
        return EsveeOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public EsveeOutput persistedOutput(final SomaticRunMetadata metadata) {

        GoogleStorageLocation unfilteredVcfLocation =
                persistedDataset.path(metadata.tumor().sampleName(), DataType.STRUCTURAL_VARIANTS_ESVEE)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(),
                                        namespace(),
                                        format("%s.%s.%s",
                                                metadata.tumor().sampleName(),
                                                "esvee.unfiltered",
                                                FileTypes.GZIPPED_VCF))));

        return EsveeOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeUnfilteredVcfLocation(unfilteredVcfLocation)
                .maybeUnfilteredVcfIndexLocation(unfilteredVcfLocation.transform(FileTypes::tabixIndex))
                .build();
    }

    private static GoogleStorageLocation resultLocation(final RuntimeBucket bucket, final ResultsDirectory resultsDirectory,
            final String filename) {
        return GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(filename)));
    }

    private static ExportPathCommand exportPathCommandFrom(final VersionedToolCommand command) {
        return new ExportPathCommand(new File(command.asBash()).getParent());
    }
}
