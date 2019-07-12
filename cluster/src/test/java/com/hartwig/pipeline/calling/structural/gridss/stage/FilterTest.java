package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import com.hartwig.pipeline.execution.vm.BashCommand;

import org.junit.Before;
import org.junit.Test;

public class FilterTest implements CommonEntities {

    private static final String PATH_TO_GRIDSS_SCRIPTS = format("%s/gridss-scripts/4.8.1", TOOLS_DIR);
    private static final int RSCRIPT_LINE_NUMBER = 3;

    private String bashCommands;
    private String originalVcf;
    private String uncompressedVcf;

    @Before
    public void setup() {
        uncompressedVcf = "/path/to/original.vcf";
        originalVcf = uncompressedVcf + ".gz";
        List<BashCommand> commands = new Filter().initialise(originalVcf, TUMOR_SAMPLE).commands();
        bashCommands = commands.stream().map(BashCommand::asBash).collect(Collectors.joining("\n"));
    }

    @Test
    public void shouldGunzipOriginalVcfAsFirstStep() {
        String firstLine = extractOutputLine(1);
        assertThat(firstLine).isEqualTo(format("gunzip -kd %s", originalVcf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfOriginalVcfFilenameDoesNotEndWithGzExtension() {
        new Filter().initialise(uncompressedVcf, TUMOR_SAMPLE);
    }

    @Test
    public void shouldRunRscriptWithCorrectScriptAsSecondStep() {
        String secondLine = extractOutputLine(RSCRIPT_LINE_NUMBER);
        assertThat(secondLine).startsWith(format("Rscript %s/gridss_somatic_filter.R ", PATH_TO_GRIDSS_SCRIPTS));
    }

    @Test
    public void shouldPassPonDirectory() {
        Map<String, String> remainingArgs = pairOffArgumentsAfterScriptPath(extractOutputLine(RSCRIPT_LINE_NUMBER));
        assertThat(remainingArgs.get("-p")).isEqualTo(RESOURCE_DIR);
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
        assertThat(bashCommands).isNotNull();
        String[] lines = bashCommands.split("\n");
        assertThat(lines[lineNo - 1]).isNotEmpty();
        return lines[lineNo - 1];
    }

    private void assertThatLinesAfterRscriptContain(String line) {
        String[] lines = bashCommands.split("\n");
        List<String> remainingLines = asList(lines).subList(RSCRIPT_LINE_NUMBER, lines.length);
        assertThat(remainingLines).contains(line);
    }
}