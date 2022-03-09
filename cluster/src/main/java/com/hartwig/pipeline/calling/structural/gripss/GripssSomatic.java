package com.hartwig.pipeline.calling.structural.gripss;

import java.util.List;

import com.hartwig.pipeline.calling.structural.gridss.GridssOutput;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;

public class GripssSomatic extends Gripss {

    public GripssSomatic(final GridssOutput gridssOutput, final PersistedDataset persistedDataset, final ResourceFiles resourceFiles) {
        super(gridssOutput, persistedDataset, GripssConfiguration.somatic(resourceFiles));
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        return Stage.disabled();
    }
}
