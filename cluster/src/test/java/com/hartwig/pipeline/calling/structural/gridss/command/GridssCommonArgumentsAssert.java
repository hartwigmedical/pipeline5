package com.hartwig.pipeline.calling.structural.gridss.command;

import org.assertj.core.api.AbstractAssert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class GridssCommonArgumentsAssert extends AbstractAssert<GridssCommonArgumentsAssert, GridssCommand> {
    private GridssArgumentsListBuilder argumentsBuilder;

    class GridssArgumentsListBuilder {
        private final List<String> args;
        private GridssCommonArgumentsAssert parent;

        public GridssArgumentsListBuilder(GridssCommonArgumentsAssert parent) {
            this.parent = parent;
            args = new ArrayList<>();
        }

        public GridssArgumentsListBuilder and(Map.Entry<String, String> pair) {
            return and(pair.getKey(), pair.getValue());
        }

        public GridssArgumentsListBuilder and(String key, String value) {
            add(key, value);
            return this;
        }

        public GridssCommonArgumentsAssert andNoMore() {
            String actualGridssArgs = actual.arguments().trim();
            if (actualGridssArgs == null || !actualGridssArgs.equals(argumentsBuilder.argsAsString())) {
                failWithMessage("Expected GRIDSS arguments to be \n<%s> \nbut got:\n<%s>", argumentsBuilder.argsAsString(),
                        actualGridssArgs);
            }

            return parent;
        }

        private void add(String key, String value) {
            args.add(format("%s=%s", key.toUpperCase(), value));
        }

        private String argsAsString() {
            return args.stream().collect(Collectors.joining(" "));
        }
    }

    GridssCommonArgumentsAssert(GridssCommand actual) {
        super(actual, GridssCommonArgumentsAssert.class);
    }

    public static GridssCommonArgumentsAssert assertThat(GridssCommand actual) {
        return new GridssCommonArgumentsAssert(actual);
    }

    public GridssCommonArgumentsAssert usesStandardAmountOfMemory() {
        int standardMemoryGb = 8;
        if (actual.memoryGb() != standardMemoryGb) {
            failWithMessage(format("Command is not using the standard %sGB of memory", standardMemoryGb));
        }
        return this;
    }

    public GridssArgumentsListBuilder hasGridssArguments(Map.Entry<String, String> pair) {
        return hasGridssArguments(pair.getKey(), pair.getValue());
    }

    public GridssArgumentsListBuilder hasGridssArguments(String key, String value) {
        GridssArgumentsListBuilder builder = new GridssArgumentsListBuilder(this);
        this.argumentsBuilder = builder;
        builder.add(key, value);
        return builder;
    }
}
