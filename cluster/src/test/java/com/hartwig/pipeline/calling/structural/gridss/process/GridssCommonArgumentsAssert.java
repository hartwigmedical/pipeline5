package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import org.assertj.core.api.AbstractAssert;

import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class GridssCommonArgumentsAssert extends AbstractAssert<GridssCommonArgumentsAssert, BashCommand> {
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
            return parent;
        }

        private void add(String key, String value) {
            args.add(format("%s=%s", key.toUpperCase(), value));
        }

        private String argsAsString() {
            return args.stream().collect(Collectors.joining(" "));
        }
    }

    GridssCommonArgumentsAssert(BashCommand actual) {
        super(actual, GridssCommonArgumentsAssert.class);
    }

    public static GridssCommonArgumentsAssert assertThat(BashCommand actual) {
        return new GridssCommonArgumentsAssert(actual);
    }

    public GridssCommonArgumentsAssert hasJvmArgsAndClassName(String className, String memory) {
        return hasJvmArgsAndClassName(Collections.emptyList(), className, memory);
    }

    public GridssCommonArgumentsAssert hasJvmArgsAndClassName(List<String> additionalJvmArgs, String className, String memory) {
        isNotNull();

        List<String> javaArgs = new ArrayList<>(Arrays.asList("java",
                "-Xmx" + memory,
                "-ea",
                "-Dsamjdk.create_index=true",
                "-Dsamjdk.use_async_io_read_samtools=true",
                "-Dsamjdk.use_async_io_write_samtools=true",
                "-Dsamjdk.use_async_io_write_tribble=true"));
        javaArgs.addAll(additionalJvmArgs);
        javaArgs.add(format("-cp %s/gridss/2.2.2/gridss.jar", VmDirectories.TOOLS));
        javaArgs.add(className);

        String expectedStart = javaArgs.stream().collect(Collectors.joining(" ")).trim();
        String actualStart =  actual.asBash().trim();
        if (!actualStart.startsWith(expectedStart)) {
            failWithMessage("Expected Java command starting with \n<%s>\nbut was:\n<%s>", expectedStart,
                    actualStart);
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

    public GridssCommonArgumentsAssert andGridssArgumentsAfterClassnameAreCorrect(String className) {
        String actualArgs[] = actual.asBash().trim().split(className);
        if (actualArgs.length != 2) {
            failWithMessage("Did not find <%s> followed by GRIDSS arguments in command line!", className);
        }
        String actualGridssArgs = actualArgs[1].trim();
        if (actualGridssArgs == null || !actualGridssArgs.equals(argumentsBuilder.argsAsString())) {
            failWithMessage("Expected GRIDSS arguments to be \n<%s> \nbut got:\n<%s>", argumentsBuilder.argsAsString(),
                    actualGridssArgs);
        }

        return this;
    }
}
