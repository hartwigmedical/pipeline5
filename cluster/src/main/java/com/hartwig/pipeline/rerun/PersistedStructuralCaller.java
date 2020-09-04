package com.hartwig.pipeline.rerun;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssPassAndPonFilter;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssSomaticFilter;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class PersistedStructuralCaller extends PersistedStage<StructuralCallerOutput, SomaticRunMetadata> {

    public PersistedStructuralCaller(final Stage<StructuralCallerOutput, SomaticRunMetadata> decorated, final Arguments arguments,
            final String persistedSet) {
        super(decorated, arguments, persistedSet);
    }

    @Override
    public StructuralCallerOutput skippedOutput(final SomaticRunMetadata metadata) {

        String somaticFilteredVcf = String.format("%s.%s.%s",
                metadata.tumor().sampleName(),
                GridssPassAndPonFilter.GRIDSS_SOMATIC_FILTERED,
                OutputFile.GZIPPED_VCF);
        String somaticVcf =
                String.format("%s.%s.%s", metadata.tumor().sampleName(), GridssSomaticFilter.GRIDSS_SOMATIC, OutputFile.GZIPPED_VCF);

        return StructuralCallerOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeFilteredVcf(GoogleStorageLocation.of(getPersistedBucket(),
                        PersistedLocations.blobForSet(getPersistedRun(), namespace(), somaticFilteredVcf)))
                .maybeFilteredVcfIndex(GoogleStorageLocation.of(getPersistedBucket(),
                        PersistedLocations.blobForSet(getPersistedRun(), namespace(), somaticFilteredVcf) + ".tbi"))
                .maybeFullVcf(GoogleStorageLocation.of(getPersistedBucket(),
                        PersistedLocations.blobForSet(getPersistedRun(), namespace(), somaticVcf)))
                .maybeFullVcfIndex(GoogleStorageLocation.of(getPersistedBucket(),
                        PersistedLocations.blobForSet(getPersistedRun(), namespace(), somaticVcf) + ".tbi"))
                .build();
    }
}
