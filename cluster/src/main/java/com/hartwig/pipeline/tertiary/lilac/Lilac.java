package com.hartwig.pipeline.tertiary.lilac;

import java.util.ArrayList;
import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
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
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

public class Lilac extends TertiaryStage<LilacOutput> {
    public static final String NAMESPACE = "lilac";

    private final ResourceFiles resourceFiles;
    private final InputDownload purpleGeneCopyNumberTsv;
    private final InputDownload purpleSomaticVcf;
    private final InputDownload purpleSomaticVcfIndex;

    public Lilac(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles, final PurpleOutput purpleOutput) {
        super(alignmentPair);
        this.resourceFiles = resourceFiles;
        this.purpleGeneCopyNumberTsv = new InputDownload(purpleOutput.outputLocations().geneCopyNumberTsv());
        this.purpleSomaticVcf = new InputDownload(purpleOutput.outputLocations().somaticVcf());
        this.purpleSomaticVcfIndex = new InputDownload(purpleOutput.outputLocations().somaticVcf().transform(x -> x + ".tbi"));
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> inputs() {
        List<BashCommand> result = new ArrayList<>(super.inputs());
        result.add(purpleGeneCopyNumberTsv);
        result.add(purpleSomaticVcf);
        result.add(purpleSomaticVcfIndex);
        return result;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        if (!FileTypes.isCram(getTumorBamDownload().getLocalTargetPath())) {
            String slicedRefBam = VmDirectories.outputFile(metadata.reference().sampleName() + ".hla.bam");
            String slicedTumorBam = VmDirectories.outputFile(metadata.tumor().sampleName() + ".hla.bam");
            return List.of(new LilacBamSliceCommand(resourceFiles, getReferenceBamDownload().getLocalTargetPath(), slicedRefBam),
                    new LilacBamIndexCommand(slicedRefBam),
                    new LilacBamSliceCommand(resourceFiles, getTumorBamDownload().getLocalTargetPath(), slicedTumorBam),
                    new LilacBamIndexCommand(slicedTumorBam),
                    new LilacCommand(resourceFiles,
                            metadata.tumor().sampleName(),
                            slicedRefBam,
                            slicedTumorBam,
                            purpleGeneCopyNumberTsv.getLocalTargetPath(),
                            purpleSomaticVcf.getLocalTargetPath()));
        } else {
            return List.of(new LilacCommand(resourceFiles,
                    metadata.tumor().sampleName(),
                    getReferenceBamDownload().getLocalTargetPath(),
                    getTumorBamDownload().getLocalTargetPath(),
                    purpleGeneCopyNumberTsv.getLocalTargetPath(),
                    purpleSomaticVcf.getLocalTargetPath()));
        }
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
        return LilacOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.LILAC_OUTPUT,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), metadata.tumor().sampleName() + ".lilac.csv")),
                        new AddDatatype(DataType.LILAC_QC_METRICS,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), metadata.tumor().sampleName() + ".lilac.qc.csv")))
                .build();
    }

    @Override
    public LilacOutput skippedOutput(final SomaticRunMetadata metadata) {
        return LilacOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public LilacOutput persistedOutput(final SomaticRunMetadata metadata) {
        return LilacOutput.builder().status(PipelineStatus.PERSISTED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow();
    }
}
