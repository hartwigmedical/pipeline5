package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import com.hartwig.pipeline.execution.vm.BashCommand;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class FilterTest implements CommonEntities {
    private static final String PATH_TO_GRIDSS_SCRIPTS = format("%s/gridss-scripts/4.8", TOOLS_DIR);
    private static final String PATH_TO_GRIDSS_PON = format("%s/gridss_pon", RESOURCE_DIR);
    private static final int RSCRIPT_LINE_NUMBER = 2;

    private BashCommand command;
    private String originalVcf;
    private String uncompressedVcf;

    @Before
    public void setup() {
        uncompressedVcf = "/path/to/original.vcf";
        originalVcf = uncompressedVcf + ".gz";
        command = new Filter().initialise(originalVcf, TUMOR_SAMPLE).command();
    }

    @Test
    public void shouldGunzipOriginalVcfAsFirstStep() {
        String firstLine = extractOutputLine(1);
        assertThat(firstLine).isEqualTo(format("gunzip -c %s > %s", originalVcf, uncompressedVcf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfOriginalVcfFilenameDoesNotEndWithGzExtension() {
        new Filter().initialise(uncompressedVcf, TUMOR_SAMPLE);
    }

    @Test
    public void shouldRunRscriptWithCorrectScriptAsSecondStep() {
        String secondLine = extractOutputLine(2);
        assertThat(secondLine).startsWith(format("Rscript %s/gridss_somatic_filter.R ", PATH_TO_GRIDSS_SCRIPTS));
    }

    @Test
    public void shouldPassPonDirectory() {
        Map<String, String> remainingArgs = pairOffArgumentsAfterScriptPath(extractOutputLine(RSCRIPT_LINE_NUMBER));
        assertThat(remainingArgs.get("-p")).isEqualTo(PATH_TO_GRIDSS_PON);
    }

    @Test
    public void shouldPassUncompressedVcfAsInputArgument() {
        Map<String, String> remainingArgs = pairOffArgumentsAfterScriptPath(extractOutputLine(RSCRIPT_LINE_NUMBER));
        assertThat(remainingArgs.get("-i")).isEqualTo(uncompressedVcf);
    }

    @Test
    public void shouldPassOutputArgument() {
        Map<String, String> remainingArgs = pairOffArgumentsAfterScriptPath(extractOutputLine(RSCRIPT_LINE_NUMBER));
        assertThat(remainingArgs.get("-o")).isEqualTo(format("%s/%s.gridss.somatic.vcf", OUT_DIR, TUMOR_SAMPLE));
    }

    @Test
    public void shouldPassScriptsDirArgument() {
        Map<String, String> remainingArgs = pairOffArgumentsAfterScriptPath(extractOutputLine(RSCRIPT_LINE_NUMBER));
        assertThat(remainingArgs.get("-s")).isEqualTo(PATH_TO_GRIDSS_SCRIPTS);
    }

    @Test
    public void shouldPassFullVcfAsFullOutputArgument() {
        Map<String, String> remainingArgs = pairOffArgumentsAfterScriptPath(extractOutputLine(RSCRIPT_LINE_NUMBER));
        assertThat(remainingArgs.get("-f")).isEqualTo(format("%s/%s.gridss.somatic.full.vcf.gz", OUT_DIR, TUMOR_SAMPLE));
    }

    @Test
    public void shouldMoveInterimFullVcfAndTbiToFinalLocationAfterRscriptRuns() {
        String fullVcf = format("%s/%s.gridss.somatic.full.vcf", OUT_DIR, TUMOR_SAMPLE);
        String resultantVcf = format("%s/%s.gridss.somatic.full.vcf.gz", OUT_DIR, TUMOR_SAMPLE);

        String moveVcf = format("mv %s.bgz %s", fullVcf, resultantVcf);
        String moveVcfTbi = format("mv %s.bgz.tbi %s.tbi", fullVcf, resultantVcf);

        assertThatLinesAfterRscriptContain(moveVcf);
        assertThatLinesAfterRscriptContain(moveVcfTbi);
    }

    @Test
    public void shouldMoveInterimFilteredVcfAndTbiToFinalLocation() {
        String outputVcf = format("%s/%s.gridss.somatic.vcf", OUT_DIR, TUMOR_SAMPLE);
        String filteredVcf = format("%s/%s.gridss.somatic.vcf.gz", OUT_DIR, TUMOR_SAMPLE);

        String moveVcf = format("mv %s.bgz %s", outputVcf, filteredVcf);
        String moveVcfTbi = format("mv %s.bgz.tbi %s.tbi", outputVcf, filteredVcf);

        assertThatLinesAfterRscriptContain(moveVcf);
        assertThatLinesAfterRscriptContain(moveVcfTbi);
    }

    private Map<String, String> pairOffArgumentsAfterScriptPath(String commandLine) {
        Map<String, String> pairs = new HashMap<>();
        String[] tokenised = commandLine.split(" +");
        assertThat(tokenised.length % 2).isEqualTo(0);
        for (int i = 2; i < tokenised.length; i += 2) {
            pairs.put(tokenised[i].trim(), tokenised[i + 1].trim());
        }
        return pairs;
    }

    private String extractOutputLine(int lineNo) {
        assertThat(command).isNotNull();
        String[] lines = command.asBash().split("\n");
        assertThat(lines[lineNo - 1]).isNotEmpty();
        return lines[lineNo - 1];
    }

    private void assertThatLinesAfterRscriptContain(String line) {
        String[] lines = command.asBash().split("\n");
        List<String> remainingLines = Arrays.asList(lines).subList(RSCRIPT_LINE_NUMBER, lines.length);
        assertThat(remainingLines).contains(line);
    }
}