package com.hartwig.pipeline.calling.sage;

import java.util.List;

import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.SubStageInputOutput;

@Namespace(SageConfiguration.SAGE_GERMLINE_NAMESPACE)
public class SageGermlineCaller extends SageCaller {

    public SageGermlineCaller(final AlignmentPair alignmentPair, final PersistedDataset persistedDataset,
            final ResourceFiles resourceFiles) {
        super(alignmentPair, persistedDataset, SageConfiguration.germline(resourceFiles));
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        return new SageApplication(sageConfiguration.commandBuilder()
                .addReference(metadata.reference().sampleName(),
                        getReferenceBamDownload().getLocalTargetPath()))
                .apply(SubStageInputOutput.empty(metadata.reference().sampleName()))
                .bash();
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        return Stage.disabled();
    }
}
