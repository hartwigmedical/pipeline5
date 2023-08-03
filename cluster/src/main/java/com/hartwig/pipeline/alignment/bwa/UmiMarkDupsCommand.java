package com.hartwig.pipeline.alignment.bwa;

import static java.lang.String.format;

import static com.hartwig.pipeline.tools.HmfTool.MARK_DUPS;

import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.execution.vm.command.java.JavaJarCommand;
import com.hartwig.pipeline.resource.ResourceFiles;

public class UmiMarkDupsCommand extends JavaJarCommand
{
    public UmiMarkDupsCommand(
            final String sampleId, final String inputBam, final ResourceFiles resourceFiles, final String outputDir, final String threads) {

        super(MARK_DUPS, formArguments(sampleId, inputBam, resourceFiles, outputDir, threads));
    }

    private static List<String> formArguments(
            final String sampleId, final String inputBam, final ResourceFiles resourceFiles, final String outputDir, final String threads) {
        List<String> arguments = Lists.newArrayList();

        arguments.add(format("-sample %s", sampleId));
        arguments.add(format("-bam_file %s", inputBam));
        arguments.add(format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(format("-ref_genome_version %s", resourceFiles.version()));
        arguments.add("-form_consensus");
        // arguments.add("-umi_enabled");
        arguments.add(format("-output_dir %s", outputDir));
        arguments.add("-log_level DEBUG"); // for now
        arguments.add(format("-threads %s", threads));

        // produces SAMPLE_ID.mark_dups.bam
        // then requires a rename, sort and index

        return arguments;
    }
}
