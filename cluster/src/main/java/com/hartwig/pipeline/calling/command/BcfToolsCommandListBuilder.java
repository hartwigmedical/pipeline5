package com.hartwig.pipeline.calling.command;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;

public class BcfToolsCommandListBuilder {

    private final String inputVcf;
    private final String outputVcf;
    private final List<BashCommand> pipedCommands;

    private boolean index;
    private String currentArguments = null;

    public BcfToolsCommandListBuilder(final String inputVcf, final String outputVcf) {
        this.inputVcf = inputVcf;
        this.outputVcf = outputVcf;
        this.pipedCommands = Lists.newArrayList();
    }

    public BcfToolsCommandListBuilder withIndex() {
        index = true;
        return this;
    }

    public BcfToolsCommandListBuilder includeHardPass() {
        return includeHardFilter("FILTER=\"PASS\"");
    }

    public BcfToolsCommandListBuilder includeHardFilter(String expression) {
        addArguments("filter", "-i", singleQuote(expression));
        return this;
    }

    public BcfToolsCommandListBuilder excludeSoftFilter(String expression, String softFilter) {
        addArguments("filter", "-e", singleQuote(expression), "-s", softFilter, "-m+");
        return this;
    }

    public BcfToolsCommandListBuilder removeAnnotation(String annotation) {
        addArguments("annotate", "-x", annotation);
        return this;
    }

    public BcfToolsCommandListBuilder addAnnotation(String file, String... annotation) {
        addArguments("annotate", "-a", file, "-c", String.join(",", annotation));
        return this;
    }

    public BcfToolsCommandListBuilder addAnnotationWithFlag(String file, String flag, String... annotation) {
        final String columns = String.join(",", annotation);
        if (columns.isEmpty()) {
            addArguments("annotate", "-a", file, "-m", flag);
        } else {
            addArguments("annotate", "-a", file, "-m", flag, "-c", columns);
        }

        return this;
    }

    public BcfToolsCommandListBuilder addAnnotationWithHeader(String file, String annotation, String header) {
        addArguments("annotate", "-a", file, "-h", header, "-c", annotation);
        return this;
    }

    public BcfToolsCommandListBuilder selectSample(String ... tumorSampleNames) {
        if (tumorSampleNames.length == 0) {
            throw new IllegalArgumentException("At least one sample must be provided");
        }

        addArguments("view", "-s", String.join(",", tumorSampleNames));
        return this;
    }

    public List<BashCommand> build() {
        if (index) {
            return ImmutableList.of(bcfCommand(), new TabixCommand(outputVcf));
        }

        return ImmutableList.of(bcfCommand());
    }

    @VisibleForTesting
    BashCommand bcfCommand() {
        if (currentArguments == null) {
            throw new IllegalStateException("No bcftools command added.");
        }

        final List<BashCommand> finalCommands = Lists.newArrayList(pipedCommands);
        final BashCommand finalCommand = new BcfToolsCommand(currentArguments, "-O", "z", "-o", outputVcf);
        finalCommands.add(finalCommand);

        return new PipeCommands(finalCommands.toArray(new BashCommand[0]));
    }

    private void addArguments(String... argumentArray) {
        final String arguments = String.join(" ", argumentArray);
        if (currentArguments == null) {
            currentArguments = String.join(" ", Lists.newArrayList(arguments, inputVcf));
        } else {
            pipedCommands.add(new BcfToolsCommand(currentArguments, "-O", "u"));
            currentArguments = arguments;
        }
    }

    private static String singleQuote(String expression) {
        String trimmed = expression.trim();

        if (!trimmed.startsWith("'")) {
            return "'" + expression + (trimmed.endsWith("'") ? "" : "'");
        }

        return expression + (trimmed.endsWith("'") ? "" : "'");
    }

}
