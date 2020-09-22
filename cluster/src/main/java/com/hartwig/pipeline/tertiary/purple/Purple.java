package com.hartwig.pipeline.tertiary.purple;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCallerPostProcessOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
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
    private final PersistedDataset persistedDataset;
    private final boolean shallow;

    public Purple(final ResourceFiles resourceFiles, SomaticCallerOutput somaticCallerOutput,
            StructuralCallerPostProcessOutput structuralCallerOutput, AmberOutput amberOutput, CobaltOutput cobaltOutput,
            final PersistedDataset persistedDataset, final boolean shallow) {
        this.resourceFiles = resourceFiles;
        somaticVcfDownload = new InputDownload(somaticCallerOutput.finalSomaticVcf());
        structuralVcfDownload = new InputDownload(structuralCallerOutput.filteredVcf());
        structuralVcfIndexDownload = new InputDownload(structuralCallerOutput.filteredVcfIndex());
        svRecoveryVcfDownload = new InputDownload(structuralCallerOutput.fullVcf());
        svRecoveryVcfIndexDownload = new InputDownload(structuralCallerOutput.fullVcfIndex());
        amberOutputDownload = new InputDownload(amberOutput.outputDirectory());
        cobaltOutputDownload = new InputDownload(cobaltOutput.outputDirectory());
        this.persistedDataset = persistedDataset;
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
    public PurpleOutput persistedOutput(final SomaticRunMetadata metadata) {
        String somaticVariantsPath = persistedDataset.file(metadata, DataType.SOMATIC_VARIANTS_PURPLE)
                .orElse(PersistedLocations.blobForSet(metadata.set(), namespace(), somaticVcf(metadata)));
        String svsPath = persistedDataset.file(metadata, DataType.STRUCTURAL_VARIANTS_PURPLE)
                .orElse(PersistedLocations.blobForSet(metadata.set(), namespace(), svVcf(metadata)));
        String outputDirectory = persistedDataset.directory(metadata, DataType.SOMATIC_VARIANTS_PURPLE)
                .orElse(PersistedLocations.pathForSet(metadata.set(), namespace()));
        return PurpleOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeOutputDirectory(GoogleStorageLocation.of(metadata.bucket(), outputDirectory, true))
                .maybeSomaticVcf(GoogleStorageLocation.of(metadata.bucket(), somaticVariantsPath))
                .maybeStructuralVcf(GoogleStorageLocation.of(metadata.bucket(), svsPath))
                .build();
    }

    private static String svVcf(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PURPLE_SV_VCF;
    }

    private static String somaticVcf(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + PURPLE_SOMATIC_VCF;
    }
}
