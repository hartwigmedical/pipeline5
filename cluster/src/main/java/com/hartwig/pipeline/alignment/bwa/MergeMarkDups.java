package com.hartwig.pipeline.alignment.bwa;

import static java.lang.String.format;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.DeleteFilesCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.SambambaCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.MvCommand;
import com.hartwig.pipeline.execution.vm.unix.RedirectStdoutCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;

import joptsimple.internal.Strings;

public class MergeMarkDups extends SubStage {

    private final String sampleId;
    private final ResourceFiles resourceFiles;
    private final List<String> inputBamPaths;

    MergeMarkDups(
            final String sampleId, final ResourceFiles resourceFiles, final List<String> inputBamPaths) {
        super("", FileTypes.BAM);
        this.sampleId = sampleId;
        this.resourceFiles = resourceFiles;
        this.inputBamPaths = inputBamPaths;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {

        List<BashCommand> cmds = Lists.newArrayList();

        String inputBam;

        if(inputBamPaths.size() > 1) {
            String inputBamStr = Strings.join(inputBamPaths, " ");
            inputBam = format("%s/%s.raw.bam", VmDirectories.OUTPUT, sampleId);
            String mergeArgs = format("merge -t %s %s %s", Bash.allCpus(), inputBam, inputBamStr);
            cmds.add(new SambambaCommand(mergeArgs));
        }
        else {
            inputBam = inputBamPaths.get(0);
        }

        // call MarkDups with UMI consensus
        cmds.add(new MarkDupsCommand(sampleId, inputBam, resourceFiles, VmDirectories.OUTPUT, Bash.allCpus()));

        // sort and index final BAM
        String markDupsBam = format("%s/%s.mark_dups.bam", VmDirectories.OUTPUT, sampleId);

        // rename final BAM
        cmds.add(new MvCommand(markDupsBam, output.path()));
        cmds.add(new MvCommand(markDupsBam + ".bai", output.path() + ".bai"));

        // delete intermediary files
        List<String> bamsToDelete = Lists.newArrayList();

        if(inputBamPaths.size() > 1) {
            bamsToDelete.add(inputBam);
            bamsToDelete.add(inputBam + ".bai");
        }

        // bamsToDelete.add(markDupsBam);

        cmds.add(new DeleteFilesCommand(bamsToDelete));

        return cmds;
    }

    private List<BashCommand> formStandardMarkDupCommands(final OutputFile output) {
        List<BashCommand> cmds = Lists.newArrayList();

        String intermediateOutputBAM = output.path() + ".intermediate.tmp";
        cmds.add(new SambambaMarkdupCommand(inputBamPaths, intermediateOutputBAM));
        cmds.add(new DeleteFilesCommand(inputBamPaths));
        cmds.add(new RedirectStdoutCommand(new BamReheaderCommand(intermediateOutputBAM), output.path()));
        cmds.add(SamtoolsCommand.index(output.path()));

        return cmds;
    }

    private List<BashCommand> formTargetedMarkDupCommands(final OutputFile output) {
        List<BashCommand> cmds = Lists.newArrayList();

        // more than 1 BAM need to be merged first but don't expect that for panel samples
        String inputBam;

        if(inputBamPaths.size() > 1) {
            String inputBamStr = Strings.join(inputBamPaths, " ");
            inputBam = format("%s/%s.raw.bam", VmDirectories.OUTPUT, sampleId);
            String mergeArgs = format("merge -t %s %s %s", Bash.allCpus(), inputBam, inputBamStr);
            cmds.add(new SambambaCommand(mergeArgs));
        }
        else {
            inputBam = inputBamPaths.get(0);
        }

        // call MarkDups with UMI consensus
        cmds.add(new MarkDupsCommand(sampleId, inputBam, resourceFiles, VmDirectories.OUTPUT, Bash.allCpus()));

        // sort and index final BAM
        String markDupsBam = format("%s/%s.mark_dups.bam", VmDirectories.OUTPUT, sampleId);
        cmds.add(SamtoolsCommand.sort(markDupsBam, output.path()));

        cmds.add(SamtoolsCommand.index(output.path()));

        // delete intermediary files
        List<String> bamsToDelete = Lists.newArrayList();

        if(inputBamPaths.size() > 1) {
            bamsToDelete.add(inputBam);
            bamsToDelete.add(inputBam + ".bai");
        }

        bamsToDelete.add(markDupsBam);

        cmds.add(new DeleteFilesCommand(bamsToDelete));

        return cmds;
    }
}
