package com.hartwig.pipeline.calling.sage;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.vm.command.BashCommand;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.SubStageInputOutput;

@Namespace(SageConfiguration.SAGE_SOMATIC_NAMESPACE)
public class SageSomaticCaller extends SageCaller {

    public SageSomaticCaller(final AlignmentPair alignmentPair, final PersistedDataset persistedDataset, final ResourceFiles resourceFiles,
            final Arguments arguments) {
        super(alignmentPair, persistedDataset, SageConfiguration.somatic(resourceFiles, arguments));
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        return new SageApplication(sageConfiguration.commandBuilder()
                .addTumor(metadata.tumor().sampleName(), getTumorBamDownload().getLocalTargetPath()))
                .andThen(sageConfiguration.postProcess().apply(metadata))
                .apply(SubStageInputOutput.empty(metadata.tumor().sampleName()))
                .bash();
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        return Stage.disabled();
    }
}
