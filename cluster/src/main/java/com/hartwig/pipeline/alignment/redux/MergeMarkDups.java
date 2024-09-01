package com.hartwig.pipeline.alignment.redux;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.computeengine.execution.vm.Bash;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;

import joptsimple.internal.Strings;

public class MergeMarkDups extends SubStage {

    private final String sampleId;
    private final ResourceFiles resourceFiles;
    private final List<String> inputBamPaths;

    public MergeMarkDups(
            final String sampleId, final ResourceFiles resourceFiles, final List<String> inputBamPaths) {
        super("", FileTypes.BAM);
        this.sampleId = sampleId;
        this.resourceFiles = resourceFiles;
        this.inputBamPaths = inputBamPaths;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {

        List<BashCommand> cmds = Lists.newArrayList();

        String inputBams = Strings.join(inputBamPaths, ",");

        cmds.add(new ReduxCommand(sampleId, inputBams, output.path(), resourceFiles, VmDirectories.OUTPUT, Bash.allCpus()));

        return cmds;
    }
}
