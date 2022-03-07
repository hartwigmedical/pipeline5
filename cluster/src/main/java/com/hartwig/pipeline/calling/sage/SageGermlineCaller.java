package com.hartwig.pipeline.calling.sage;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.resource.ResourceFiles;

public class SageGermlineCaller extends SageCaller {

    public SageGermlineCaller(final AlignmentPair alignmentPair, final PersistedDataset persistedDataset,
            final ResourceFiles resourceFiles) {
        super(alignmentPair, persistedDataset, SageConfiguration.germline(resourceFiles));
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        return Collections.emptyList();
    }
}
