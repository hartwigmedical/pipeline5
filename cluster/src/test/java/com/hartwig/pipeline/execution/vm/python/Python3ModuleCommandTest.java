package com.hartwig.pipeline.execution.vm.python;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

public class Python3ModuleCommandTest
{
    @Test
    public void shouldConstructFullCommandLineFromToolNameAndVersion() {
        Python3ModuleCommand command = new Python3ModuleCommand(
                "cuppa",
                "2.0",
                "cuppa.predict",
                List.of(
                        "--features_path=./tumor_sample.cuppa_data.tsv.gz",
                        "--output_dir=./dir/"
                )
        );

        String expectedCommand = "source /opt/tools/cuppa/2.0_venv/bin/activate; "
                + "python -m cuppa.predict "
                + "--features_path=./tumor_sample.cuppa_data.tsv.gz "
                + "--output_dir=./dir/; "
                + "deactivate";

        assertThat(command.asBash()).isEqualTo(expectedCommand);
    }
}