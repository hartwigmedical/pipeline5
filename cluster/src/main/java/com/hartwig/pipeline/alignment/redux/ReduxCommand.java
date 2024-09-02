package com.hartwig.pipeline.alignment.redux;

import static java.lang.String.format;

import static com.hartwig.pipeline.tools.HmfTool.REDUX;

import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.java.JavaJarCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.ExternalTool;

public class ReduxCommand extends JavaJarCommand
{
    public ReduxCommand(
            final String sampleId, final String inputBam, final String outputBam, final ResourceFiles resourceFiles,
            final String outputDir, final String threads) {

        super(
                REDUX.getToolName(), REDUX.runVersion(), REDUX.jar(), REDUX.maxHeapStr(),
                formArguments(sampleId, inputBam, outputBam, resourceFiles, outputDir, threads));
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
        // arguments.add("-use_supp_bam");
        arguments.add(format("-bamtool %s", ExternalTool.SAMBAMBA.binaryPath()));
        arguments.add(format("-output_bam %s", outputBam));
        arguments.add(format("-output_dir %s", outputDir));
        arguments.add("-log_debug"); // for now
        arguments.add(format("-threads %s", threads));

        return arguments;
    }
}
