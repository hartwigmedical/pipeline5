package com.hartwig.pipeline.tertiary.peach;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.python.Python3Command;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tools.Versions;

public class Peach implements Stage<PeachOutput, SomaticRunMetadata> {
    static final String NAMESPACE = "peach";
    private static final String PEACH_CALLS_TSV = ".peach.calls.tsv";
    private static final String PEACH_GENOTYPE_TSV = ".peach.genotype.tsv";

    private final InputDownload purpleGermlineVcfDownload;
    private final ResourceFiles resourceFiles;

    public Peach(final PurpleOutput purpleOutput, final ResourceFiles resourceFiles) {
        purpleGermlineVcfDownload = new InputDownload(purpleOutput.outputLocations().germlineVcf());
        this.resourceFiles = resourceFiles;
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(purpleGermlineVcfDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return List.of(new Python3Command("peach",
                Versions.PEACH,
                "src/main.py",
                List.of(purpleGermlineVcfDownload.getLocalTargetPath(),
                        metadata.tumor().sampleName(),
                        metadata.reference().sampleName(),
                        Versions.PEACH,
                        VmDirectories.OUTPUT,
                        resourceFiles.peachFilterBed(),
                        "/usr/bin/vcftools")));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .name(NAMESPACE)
                .startupCommand(bash)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(2, 4))
                .workingDiskSpaceGb(375)
                .build();
    }

    @Override
    public PeachOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return PeachOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.PEACH_CALLS_TSV,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), metadata.tumor().sampleName() + PEACH_CALLS_TSV)),
                        new AddDatatype(DataType.PEACH_GENOTYPE_TSV,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), metadata.tumor().sampleName() + PEACH_GENOTYPE_TSV)))
                .build();
    }

    @Override
    public PeachOutput skippedOutput(final SomaticRunMetadata metadata) {
        return PeachOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return !arguments.shallow() && arguments.runTertiary();
    }
}
