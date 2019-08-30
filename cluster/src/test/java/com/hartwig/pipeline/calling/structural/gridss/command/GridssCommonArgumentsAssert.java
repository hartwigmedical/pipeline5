package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;
import static java.util.Arrays.copyOfRange;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.AbstractAssert;

public class GridssCommonArgumentsAssert extends AbstractAssert<GridssCommonArgumentsAssert, GridssCommand> {
    private GridssArgumentsListBuilder argumentsBuilder;


    class GridssArgumentsListBuilder {
        private final List<String> gridssArgs;
        private GridssCommonArgumentsAssert parent;

        public GridssArgumentsListBuilder(GridssCommonArgumentsAssert parent) {
            this.parent = parent;
            gridssArgs = new ArrayList<>();
        }

        public GridssArgumentsListBuilder and(Map.Entry<String, String> pair) {
            return and(pair.getKey(), pair.getValue());
        }

        public GridssArgumentsListBuilder and(String key, String value) {
            add(key, value);
            return this;
        }

        public GridssArgumentsListBuilder andConfigFile(String configFile) {
            return and("configuration_file", configFile);
        }

        public GridssArgumentsListBuilder andBlacklist(String blacklist) {
            return and("blacklist", blacklist);
        }

        public GridssCommonArgumentsAssert andNoMore() {
            String bash = actual.asBash().trim();
            String className = actual.className();
            String actualGridssArgs = bash.substring(bash.indexOf(className) + className.length()).trim();
            if (!actualGridssArgs.equals(argumentsBuilder.argsAsString())) {
                failWithMessage("Expected GRIDSS arguments to be \n<%s> \nbut got:\n<%s>", argumentsBuilder.argsAsString(),
                        actualGridssArgs);
            }

            return parent;
        }

        private void add(String key, String value) {
            gridssArgs.add(format("%s=%s", key.toUpperCase(), value));
        }

        private String argsAsString() {
            return gridssArgs.stream().collect(joining(" "));
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

    public GridssCommonArgumentsAssert usesMaxHeapSizeGb(int sizeGb) {
        if (actual.asBash().startsWith("java -Xmx" + sizeGb + "G")) {
            failWithMessage("Command is not set to have max " + sizeGb + "G heap");
        }
        return this;
    }

    public GridssCommonArgumentsAssert generatesJavaInvocationUpToAndIncludingClassname(String className) {
        String invocation = actual.asBash();
        String[] javaArgs = invocation.split(format(" %s ", className));
        if (javaArgs.length < 2) {
            failWithMessage(format("Did not find className '%s' in generated invocation: %s", className,  invocation));
        }
        if (javaArgs.length > 2) {
            failWithMessage(format("Found '%s' more than once, something is not right: %s", className, invocation));
        }
        String[] tokens = javaArgs[0].split(" +");
        if (!tokens[0].equals("java")) {
            failWithMessage(format("'java' needs to be the first token in the invocation but it isn't: '%s'", invocation));
        }
        if (!tokens[1].matches("-Xmx\\d+G")) {
            failWithMessage(format("Heap size in GB needs to be the second token but it is not: '%s'", invocation));
        }

        ArrayList<String> expectedJvmProperties = new ArrayList<>(Arrays.asList("-Dsamjdk.create_index=true",
                "-Dsamjdk.use_async_io_read_samtools=true",
                "-Dsamjdk.use_async_io_write_samtools=true",
                "-Dsamjdk.use_async_io_write_tribble=true",
                "-Dsamjdk.buffer_size=4194304"
        ));

        String actualJvmProperties = " " + stream(copyOfRange(tokens, 2, tokens.length)).collect(joining(" ")) + " ";
        for (String expectedRemainingToken: expectedJvmProperties) {
            if (!actualJvmProperties.contains(format(" %s ", expectedRemainingToken))) {
                failWithMessage("Invocation does not contain required argument '%s': '%s'", expectedRemainingToken, invocation);
            }
        }

        String classpathTokens = "-cp /data/tools/gridss/2.5.2/gridss.jar";
        if (!actualJvmProperties.trim().endsWith(classpathTokens)) {
            failWithMessage(format("Did not find classpath entry \"%s\" for GRIDSS JAR in invocation: %s",
                    classpathTokens, invocation));
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
