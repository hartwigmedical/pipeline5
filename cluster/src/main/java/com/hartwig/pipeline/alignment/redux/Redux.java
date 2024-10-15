package com.hartwig.pipeline.alignment.redux;

import static java.lang.String.format;

import static com.hartwig.pipeline.tools.HmfTool.REDUX;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.computeengine.execution.vm.Bash;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.java.JavaJarCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.tools.ExternalTool;

import joptsimple.internal.Strings;

public class Redux extends SubStage {

    private final String sampleId;
    private final ResourceFiles resourceFiles;
    private final List<String> inputBamPaths;

    public static final String REDUX_JITTER_PARAMS_TSV = ".jitter_params.tsv";
    public static final String REDUX_MS_TABLE_TSV = ".ms_table.tsv.gz";
    public static final String REDUX_REPEAT_TSV = ".repeat.tsv.gz";

    public Redux(
            final String sampleId, final ResourceFiles resourceFiles, final List<String> inputBamPaths) {
        super("", FileTypes.BAM);
        this.sampleId = sampleId;
        this.resourceFiles = resourceFiles;
        this.inputBamPaths = inputBamPaths;
    }

    public static String jitterParamsTsv(final String sampleId) {
        return sampleId + REDUX_MS_TABLE_TSV;
    }
    public static String msTableTsv(final String sampleId) {
        return sampleId + REDUX_JITTER_PARAMS_TSV;
    }
    public static String repeatTsv(final String sampleId) {
        return sampleId + REDUX_REPEAT_TSV;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {

        List<BashCommand> cmds = Lists.newArrayList();

        String inputBams = Strings.join(inputBamPaths, ",");

        List<String> arguments = formArguments(sampleId, inputBams, output.path(), resourceFiles, VmDirectories.OUTPUT, Bash.allCpus());

        cmds.add(new JavaJarCommand(REDUX.getToolName(), REDUX.runVersion(), REDUX.jar(), REDUX.maxHeapStr(), arguments));

        return cmds;
    }

    private static List<String> formArguments(
            final String sampleId, final String inputBam, final String outputBam, final ResourceFiles resourceFiles,
            final String outputDir, final String threads) {

        List<String> arguments = Lists.newArrayList();

        arguments.add(format("-sample %s", sampleId));
        arguments.add(format("-input_bam %s", inputBam));
        arguments.add(format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(format("-ref_genome_version %s", resourceFiles.version()));
        arguments.add(format("-unmap_regions %s", resourceFiles.unmapRegionsFile()));
        arguments.add(format("-ref_genome_msi_file %s", resourceFiles.msiJitterSitesFile()));
        arguments.add("-form_consensus");
        arguments.add("-use_supp_bam");
        arguments.add(format("-bamtool %s", ExternalTool.SAMBAMBA.binaryPath()));
        arguments.add(format("-output_bam %s", outputBam));
        arguments.add(format("-output_dir %s", outputDir));
        arguments.add("-log_debug"); // for now
        arguments.add(format("-threads %s", threads));

        return arguments;
    }
}
