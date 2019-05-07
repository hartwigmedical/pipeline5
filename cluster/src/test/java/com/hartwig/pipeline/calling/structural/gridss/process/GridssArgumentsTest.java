package com.hartwig.pipeline.calling.structural.gridss.process;

import org.junit.Test;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class GridssArgumentsTest {
    @Test
    public void shouldReturnArgumentsAsString() {
        String keyOne = "some_key";
        String keyTwo = "some_other_key";
        String valueOne = "ValueOne";
        String valueTwo = "ValueTwo";

        GridssArgument argOne = new GridssArgument(keyOne, valueOne);
        GridssArgument argTwo = new GridssArgument(keyTwo, valueTwo);

        GridssArguments args = new GridssArguments();
        args.add(keyOne, valueOne).add(keyTwo, valueTwo);

        assertThat(args.asBash()).isEqualTo(format("%s %s", argOne.asBash(), argTwo.asBash()));
    }
}
