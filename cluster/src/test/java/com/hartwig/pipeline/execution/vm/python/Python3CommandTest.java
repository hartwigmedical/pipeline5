package com.hartwig.pipeline.execution.vm.python;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.hartwig.pipeline.execution.vm.command.python.Python3Command;

import org.junit.Test;

public class Python3CommandTest {
    @Test
    public void shouldConstructFullCommandLineFromToolNameAndVersion() {
        Python3Command victim = new Python3Command("toolName", "1.1", "src/main.py", List.of("a", "b", "c"));
        assertThat(victim.asBash()).isEqualTo("/opt/tools/toolName/1.1_venv/bin/python /opt/tools/toolName/1.1/src/main.py a b c");
    }
}