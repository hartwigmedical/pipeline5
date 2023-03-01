package com.hartwig.pipeline.metrics;

import static java.lang.String.format;

import java.util.Collections;
import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;

import org.jetbrains.annotations.Nullable;

class BamMetricsCommand extends JavaClassCommand {

    BamMetricsCommand(
            final String sampleId, final String inputBam, final ResourceFiles resourceFiles, final String outputDir, final String threads,
            @Nullable final String targetRegionsBed) {

        super("bam-tools",
                Versions.BAM_TOOLS,
                "bam-tools.jar",
                "com.hartwig.hmftools.bamtools.metrics.BamMetrics",
                "24G",
                Collections.emptyList(),
                formArguments(sampleId, inputBam, resourceFiles, outputDir, threads, targetRegionsBed));
    }

    private static List<String> formArguments(
            final String sampleId, final String inputBam, final ResourceFiles resourceFiles, final String outputDir, final String threads,
            @Nullable final String targetRegionsBed)
    {
        List<String> arguments = Lists.newArrayList();

        arguments.add(format("-sample %s", sampleId));
        arguments.add(format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(format("-ref_genome_version %s", resourceFiles.version()));
        arguments.add(format("-bam_file %s", inputBam));
        arguments.add(format("-output_dir %s", outputDir));
        arguments.add("-log_level INFO");
        arguments.add(format("-threads %s", threads));
        arguments.add("-write_old_style");

        if(targetRegionsBed != null)
            arguments.add(format("-regions_bed_file %s", targetRegionsBed));

        return arguments;
    }
}