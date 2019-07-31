package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;

import org.junit.Test;

public class RscriptFilterTest implements CommonEntities {
    @Test
    public void shouldGenerateCommand() {
        String inputFile = "input.vcf";
        String outputFile = OUT_DIR + "/output.vcf";
        String fullCompressedVcf = "full.vcf.gz";
        String pathToScripts = TOOLS_DIR + "/gridss-scripts/4.8.1";

        String expected = format("Rscript %s/gridss_somatic_filter.R -p %s -i %s -o %s -f %s -s %s",
                pathToScripts, RESOURCE_DIR, inputFile, outputFile, fullCompressedVcf, pathToScripts);

        assertThat(new RscriptFilter(inputFile, outputFile, fullCompressedVcf).asBash()).isEqualTo(expected);
    }
}