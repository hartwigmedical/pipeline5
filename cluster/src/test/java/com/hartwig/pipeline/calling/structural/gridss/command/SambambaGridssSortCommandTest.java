package com.hartwig.pipeline.calling.structural.gridss.command;

import org.junit.Before;
import org.junit.Test;

import static com.hartwig.pipeline.calling.structural.gridss.GridssTestConstants.OUTPUT_BAM;
import static com.hartwig.pipeline.testsupport.TestConstants.PROC_COUNT;
import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_SAMBAMBA;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class SambambaGridssSortCommandTest {
    private String common;
    private String input;

    @Before
    public void setup() {
        common = format("%s sort -m 8G -t " + PROC_COUNT + " -l 0 -o %s", TOOLS_SAMBAMBA, OUTPUT_BAM);
        input = "/dev/stdin";
    }

    @Test
    public void shouldCreateSambambaCommandWithDefaultSorting() {
        SambambaGridssSortCommand command = SambambaGridssSortCommand.sortByDefault(OUTPUT_BAM);
        assertThat(command.asBash()).isNotEmpty();
        assertThat(command.asBash()).isEqualTo(common + " " + input);
    }

    @Test
    public void shouldCreateSambambaCommandWithSortingByName() {
        SambambaGridssSortCommand command = SambambaGridssSortCommand.sortByName(OUTPUT_BAM);
        assertThat(command.asBash()).isNotEmpty();
        assertThat(command.asBash()).isEqualTo(format("%s -n %s", common, input));
    }
}