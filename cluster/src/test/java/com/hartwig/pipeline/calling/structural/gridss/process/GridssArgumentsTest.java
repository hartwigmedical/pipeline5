package com.hartwig.pipeline.calling.structural.gridss.process;

import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class GridssArgumentsTest {
    private GridssArguments args;
    private GridssArgument argOne;

    @Before
    public void setup() {
        String keyOne = "some_key";
        String valueOne = "ValueOne";

        args = new GridssArguments();
        args.add(keyOne, valueOne);
        argOne = new GridssArgument(keyOne, valueOne);
    }

    @Test
    public void shouldReturnArgumentsAsString() {
        String keyTwo = "some_other_key";
        String valueTwo = "ValueTwo";

        GridssArgument argTwo = new GridssArgument(keyTwo, valueTwo);
        args.add(keyTwo, valueTwo);

        assertThat(args.asBash()).isEqualTo(format("%s %s", argOne.asBash(), argTwo.asBash()));
    }

    @Test
    public void shouldAddTempdirArgument() {
        String expectedKey = "tmp_dir";
        String expectedValue = "/tmp";

        GridssArguments returnedArgs = args.addTempDir();

        assertThat(returnedArgs).isSameAs(args);
        assertThat(args.asBash()).isEqualTo(format("%s %s", argOne.asBash(), new GridssArgument(expectedKey, expectedValue).asBash()));
    }
}
