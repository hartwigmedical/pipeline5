package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.tools.Versions;

import org.junit.Test;

public class RscriptFilterTest {
    @Test
    public void shouldGenerateCommand() {
        String inputFile = "input.vcf";
        String outputFile = VmDirectories.outputFile("/output.vcf");
        String fullCompressedVcf = "full.vcf.gz";
        String pathToScripts = "/opt/tools/gridss/" + Versions.GRIDSS;

        String expected = format("Rscript %s/gridss_somatic_filter.R -p /opt/resources -i %s -o %s -f %s -s %s",
                pathToScripts,
                inputFile,
                outputFile,
                fullCompressedVcf,
                pathToScripts);

        assertThat(new RscriptFilter(inputFile, outputFile, fullCompressedVcf).asBash()).isEqualTo(expected);
    }
}