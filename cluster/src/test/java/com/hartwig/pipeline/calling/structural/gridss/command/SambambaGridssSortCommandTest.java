package com.hartwig.pipeline.calling.structural.gridss.command;

import org.junit.Before;
import org.junit.Test;

import static com.hartwig.pipeline.calling.structural.gridss.CommonEntities.OUTPUT_BAM;
import static com.hartwig.pipeline.calling.structural.gridss.CommonEntities.PATH_TO_SAMBAMBA;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class SambambaGridssSortCommandTest {
    private String common;
    private String input;

    @Before
    public void setup() {
        common = format("%s sort -t $(grep -c '^processor' /proc/cpuinfo) -l 0 -o %s", PATH_TO_SAMBAMBA, OUTPUT_BAM);
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