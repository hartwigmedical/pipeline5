package com.hartwig.pipeline.calling.structural.gridss.command;

import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.*;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class RscriptFilterTest {
    @Test
    public void shouldGenerateCommand() {
        String inputFile = "input.vcf";
        String outputFile = outFile("/output.vcf");
        String fullCompressedVcf = "full.vcf.gz";
        String pathToScripts = TOOLS_DIR + "/gridss-scripts/4.8.1";

        String expected = format("Rscript %s/gridss_somatic_filter.R -p %s -i %s -o %s -f %s -s %s",
                pathToScripts, RESOURCE_DIR, inputFile, outputFile, fullCompressedVcf, pathToScripts);

        assertThat(new RscriptFilter(inputFile, outputFile, fullCompressedVcf).asBash()).isEqualTo(expected);
    }
}