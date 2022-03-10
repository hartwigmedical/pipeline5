package com.hartwig.pipeline.calling.sage;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;

public class SageSomaticCaller extends SageCaller {

    public SageSomaticCaller(final AlignmentPair alignmentPair, final PersistedDataset persistedDataset, final ResourceFiles resourceFiles,
            final boolean isShallow) {
        super(alignmentPair, persistedDataset, SageConfiguration.somatic(resourceFiles, isShallow));
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        return Stage.disabled();
    }
}
