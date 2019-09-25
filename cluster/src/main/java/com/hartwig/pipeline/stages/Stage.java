package com.hartwig.pipeline.stages;

import java.util.List;
import java.util.Map;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.RunMetadata;
import com.hartwig.pipeline.storage.RuntimeBucket;

public interface Stage<S extends StageOutput, M extends RunMetadata> {

    List<BashCommand> inputs();

    List<ResourceDownload> resources(Storage storage, String resourceBucket, RuntimeBucket bucket);

    String namespace();

    List<BashCommand> commands(M metadata, Map<String, ResourceDownload> resources);

    VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory);

    S output(final M metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory);

    S skippedOutput(M metadata);

    boolean shouldRun(Arguments arguments);
}
