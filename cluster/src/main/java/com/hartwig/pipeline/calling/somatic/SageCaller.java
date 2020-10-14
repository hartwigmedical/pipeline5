package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.UnzipToDirectoryCommand;
import com.hartwig.pipeline.metadata.AddDatatypeToFile;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.ReportComponent;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.report.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;

public class SageCaller extends TertiaryStage<SomaticCallerOutput> {

    public static final String NAMESPACE = "sage";

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;
    private OutputFile filteredOutputFile;
    private OutputFile unfilteredOutputFile;

    public SageCaller(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        super(alignmentPair);
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {

        List<BashCommand> commands = Lists.newArrayList();
        commands.add(new UnzipToDirectoryCommand(VmDirectories.RESOURCES, resourceFiles.snpEffDb()));

        String tumorBamPath = getTumorBamDownload().getLocalTargetPath();
        String referenceBamPath = getReferenceBamDownload().getLocalTargetPath();
        String tumorSampleName = metadata.tumor().sampleName();
        String referenceSampleName = metadata.reference().sampleName();

        SageCommandBuilder sageCommandBuilder = new SageCommandBuilder(resourceFiles).addReference(referenceSampleName, referenceBamPath)
                .addTumor(tumorSampleName, tumorBamPath);
        SageApplication sageApplication = new SageApplication(sageCommandBuilder);
        SagePostProcess sagePostProcess = new SagePostProcess(tumorSampleName, resourceFiles);

        SubStageInputOutput sageOutput = sageApplication.andThen(sagePostProcess).apply(SubStageInputOutput.empty(tumorSampleName));
        commands.addAll(sageOutput.bash());

        unfilteredOutputFile = sageApplication.apply(SubStageInputOutput.empty(tumorSampleName)).outputFile();
        filteredOutputFile = sageOutput.outputFile();
        return commands;
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.sageCalling(bash, resultsDirectory);
    }

    @Override
    public SomaticCallerOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return SomaticCallerOutput.builder(NAMESPACE)
                .status(jobStatus)
                .maybeFinalSomaticVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(filteredOutputFile.fileName())))
                .addReportComponents(bqrComponent(metadata.tumor(), "png", bucket, resultsDirectory))
                .addReportComponents(bqrComponent(metadata.tumor(), "tsv", bucket, resultsDirectory))
                .addReportComponents(bqrComponent(metadata.reference(), "png", bucket, resultsDirectory))
                .addReportComponents(bqrComponent(metadata.reference(), "tsv", bucket, resultsDirectory))
                .addReportComponents(vcfComponent(unfilteredOutputFile.fileName(), bucket, resultsDirectory))
                .addReportComponents(vcfComponent(filteredOutputFile.fileName(), bucket, resultsDirectory))
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.root(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.root()))
                .addFurtherOperations(new AddDatatypeToFile(DataType.SOMATIC_VARIANTS_SAGE,
                        Folder.root(),
                        namespace(),
                        filteredOutputFile.fileName(),
                        metadata.barcode()))
                .build();
    }

    @Override
    public SomaticCallerOutput skippedOutput(final SomaticRunMetadata metadata) {
        return SomaticCallerOutput.builder(NAMESPACE).status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runSomaticCaller();
    }

    @Override
    public SomaticCallerOutput persistedOutput(final SomaticRunMetadata metadata) {
        String vcfPath = persistedDataset.file(metadata, DataType.SOMATIC_VARIANTS_SAGE)
                .orElse(PersistedLocations.blobForSet(metadata.set(),
                        namespace(),
                        String.format("%s.%s.%s",
                                metadata.tumor().sampleName(),
                                SagePostProcess.SAGE_SOMATIC_FILTERED,
                                FileTypes.GZIPPED_VCF)));
        return SomaticCallerOutput.builder(namespace())
                .status(PipelineStatus.PERSISTED)
                .maybeFinalSomaticVcf(GoogleStorageLocation.of(metadata.bucket(), vcfPath))
                .build();
    }

    private ReportComponent bqrComponent(final SingleSampleRunMetadata metadata, final String extension, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        String filename = String.format("%s.sage.bqr.%s", metadata.sampleName(), extension);
        return new SingleFileComponent(bucket, NAMESPACE, Folder.root(), filename, filename, resultsDirectory);
    }

    private ReportComponent vcfComponent(final String filename, final RuntimeBucket bucket, final ResultsDirectory resultsDirectory) {
        return new ZippedVcfAndIndexComponent(bucket, NAMESPACE, Folder.root(), filename, resultsDirectory);
    }
}