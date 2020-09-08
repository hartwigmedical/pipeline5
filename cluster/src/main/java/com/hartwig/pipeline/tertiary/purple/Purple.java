package com.hartwig.pipeline.tertiary.purple;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.startingpoint.PersistedLocations;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tools.Versions;

public class Purple implements Stage<PurpleOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "purple";
    public static final String PURPLE_SOMATIC_VCF = ".purple.somatic.vcf.gz";
    public static final String PURPLE_SV_VCF = ".purple.sv.vcf.gz";

    private final ResourceFiles resourceFiles;
    private final InputDownload somaticVcfDownload;
    private final InputDownload structuralVcfDownload;
    private final InputDownload structuralVcfIndexDownload;
    private final InputDownload svRecoveryVcfDownload;
    private final InputDownload svRecoveryVcfIndexDownload;
    private final InputDownload amberOutputDownload;
    private final InputDownload cobaltOutputDownload;
    private final boolean shallow;

    public Purple(final ResourceFiles resourceFiles, SomaticCallerOutput somaticCallerOutput, StructuralCallerOutput structuralCallerOutput,
            AmberOutput amberOutput, CobaltOutput cobaltOutput, final boolean shallow) {
        this.resourceFiles = resourceFiles;
        somaticVcfDownload = new InputDownload(somaticCallerOutput.finalSomaticVcf());
        structuralVcfDownload = new InputDownload(structuralCallerOutput.filteredVcf());
        structuralVcfIndexDownload = new InputDownload(structuralCallerOutput.filteredVcfIndex());
        svRecoveryVcfDownload = new InputDownload(structuralCallerOutput.fullVcf());
        svRecoveryVcfIndexDownload = new InputDownload(structuralCallerOutput.fullVcfIndex());
        amberOutputDownload = new InputDownload(amberOutput.outputDirectory());
        cobaltOutputDownload = new InputDownload(cobaltOutput.outputDirectory());
        this.shallow = shallow;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return Collections.singletonList(new PurpleApplicationCommand(resourceFiles,
                metadata.reference().sampleName(),
                metadata.tumor().sampleName(),
                amberOutputDownload.getLocalTargetPath(),
                cobaltOutputDownload.getLocalTargetPath(),
                somaticVcfDownload.getLocalTargetPath(),
                structuralVcfDownload.getLocalTargetPath(),
                svRecoveryVcfDownload.getLocalTargetPath(),
                VmDirectories.TOOLS + "/circos/" + Versions.CIRCOS + "/bin/circos",
                shallow));
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(somaticVcfDownload,
                structuralVcfDownload,
                structuralVcfIndexDownload,
                svRecoveryVcfDownload,
                svRecoveryVcfIndexDownload,
                amberOutputDownload,
                cobaltOutputDownload);
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.purple(bash, resultsDirectory);
    }

    @Override
    public PurpleOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return PurpleOutput.builder()
                .status(jobStatus)
                .maybeOutputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                .maybeSomaticVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(somaticVcf(metadata))))
                .maybeStructuralVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(svVcf(metadata))))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.from(), NAMESPACE, resultsDirectory))
                .build();
    }

    @Override
    public PurpleOutput skippedOutput(final SomaticRunMetadata metadata) {
        return PurpleOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public PurpleOutput persistedOutput(final String persistedBucket, final String persistedRun, final SomaticRunMetadata metadata) {
        return PurpleOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeOutputDirectory(GoogleStorageLocation.of(persistedBucket,
                        PersistedLocations.pathForSet(persistedRun, namespace()),
                        true))
                .maybeSomaticVcf(GoogleStorageLocation.of(persistedBucket,
                        PersistedLocations.blobForSet(persistedRun, namespace(), somaticVcf(metadata))))
                .maybeStructuralVcf(GoogleStorageLocation.of(persistedBucket,
                        PersistedLocations.blobForSet(persistedRun, namespace(), svVcf(metadata))))
                .build();
    }

    private static String svVcf(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PURPLE_SV_VCF;
    }

    private static String somaticVcf(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PURPLE_SOMATIC_VCF;
    }
}
