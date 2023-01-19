package com.hartwig.pipeline.alignment.bwa;

import java.util.List;

import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.DeleteFilesCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.unix.RedirectStdoutCommand;
import com.hartwig.pipeline.stages.SubStage;

public class MergeMarkDups extends SubStage {

    private final List<String> inputBamPaths;

    MergeMarkDups(final List<String> inputBamPaths) {
        super("", FileTypes.BAM);
        this.inputBamPaths = inputBamPaths;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        List<BashCommand> cmds = new java.util.ArrayList<>();
        String intermediateOutputBAM = output.path() + ".intermediate.tmp";
        cmds.add(new SambambaMarkdupCommand(inputBamPaths, intermediateOutputBAM));
        cmds.add(new DeleteFilesCommand(inputBamPaths));
        cmds.add(new RedirectStdoutCommand(new BamReheaderCommand(intermediateOutputBAM),  output.path()));
        cmds.add(new SamtoolsCommand("index", output.path()));
        return cmds;
    }
}
