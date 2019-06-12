package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GridssToBashCommandConverterTest {
    @Test
    public void shouldSetOptionsInJavaCommandFromGridssCommand() {
        GridssCommand gridssCommand = mock(GridssCommand.class);
        int memoryGb = 24;
        String memoryAsString = format("%dG", memoryGb);
        String className = "some.class.Name";
        String arguments = "these=are the=arguments";

        List<String> jvmArgs = asList(
                "-ea",
                "-Dsamjdk.create_index=true",
                "-Dsamjdk.use_async_io_read_samtools=true",
                "-Dsamjdk.use_async_io_write_samtools=true",
                "-Dsamjdk.use_async_io_write_tribble=true",
                "-Dsamjdk.buffer_size=4194304"
        );

        when(gridssCommand.memoryGb()).thenReturn(memoryGb);
        when(gridssCommand.className()).thenReturn(className);
        when(gridssCommand.arguments()).thenReturn(arguments);

        GridssToBashCommandConverter converter = new GridssToBashCommandConverter();
        JavaClassCommand command = converter.convert(gridssCommand);

        String bash = command.asBash();
        assertThat(bash).isNotEmpty();
        assertThat(bash).isEqualTo(String.format("java -Xmx%s %s -cp %s/gridss/2.2.2/gridss.jar %s %s",
                memoryAsString,
                jvmArgs.stream().collect(Collectors.joining(" ")),
                CommonEntities.TOOLS_DIR,
                className,
                arguments));
    }
}