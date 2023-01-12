package com.hartwig.pipeline.alignment.bwa;

import java.util.List;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.unix.RedirectStdoutCommand;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.tools.Versions;

public class MergeMarkDups extends SubStage {

    private final List<String> inputBamPaths;

    MergeMarkDups(final List<String> inputBamPaths) {
        super("", FileTypes.BAM);
        this.inputBamPaths = inputBamPaths;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        List<BashCommand> cmds = new java.util.ArrayList<>();
        String finalOutputBAM = output.path();
        String intermediateOutputBAM = output.path() + ".tmp";
        cmds.add(new SambambaMarkdupCommand(inputBamPaths, intermediateOutputBAM));
        cmds.add(new RedirectStdoutCommand(new VersionedToolCommand("samtools",
                "samtools",
                Versions.SAMTOOLS,
                "reheader",
                "--no-PG",
                "--command",
                "'grep -v ^@PG'",
                intermediateOutputBAM), finalOutputBAM));
        return cmds;
    }
}
