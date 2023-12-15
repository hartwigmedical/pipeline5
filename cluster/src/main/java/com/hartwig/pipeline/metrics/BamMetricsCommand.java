package com.hartwig.pipeline.metrics;

import static java.lang.String.format;

import static com.hartwig.pipeline.tools.HmfTool.BAM_TOOLS;

import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.computeengine.execution.vm.command.java.JavaJarCommand;
import com.hartwig.pipeline.resource.ResourceFiles;

import org.jetbrains.annotations.Nullable;

class BamMetricsCommand extends JavaJarCommand {
    BamMetricsCommand(final String sampleId, final String inputBam, final ResourceFiles resourceFiles, final String outputDir,
            final String threads, @Nullable final String targetRegionsBed) {

        super(BAM_TOOLS.getToolName(),
                BAM_TOOLS.getVersion(),
                BAM_TOOLS.jar(),
                BAM_TOOLS.maxHeapStr(),
                formArguments(sampleId, inputBam, resourceFiles, outputDir, threads, targetRegionsBed));
    }

    private static List<String> formArguments(final String sampleId, final String inputBam, final ResourceFiles resourceFiles,
            final String outputDir, final String threads, @Nullable final String targetRegionsBed) {
        List<String> arguments = Lists.newArrayList();

        arguments.add(format("-sample %s", sampleId));
        arguments.add(format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(format("-ref_genome_version %s", resourceFiles.version()));
        arguments.add(format("-bam_file %s", inputBam));
        arguments.add(format("-output_dir %s", outputDir));
        arguments.add("-log_level INFO");
        arguments.add(format("-threads %s", threads));
        arguments.add("-write_old_style");

        if (targetRegionsBed != null) {
            arguments.add(format("-regions_bed_file %s", targetRegionsBed));
        }

        return arguments;
    }
}