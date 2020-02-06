package com.hartwig.pipeline.calling.command;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;

public class BcfToolsCommandBuilder  {

    private final String inputVcf;
    private final String outputVcf;
    private final List<BashCommand> pipedCommands = Lists.newArrayList();
    private String currentArguments = null;

    public BcfToolsCommandBuilder(final String inputVcf, final String outputVcf) {
        this.inputVcf = inputVcf;
        this.outputVcf = outputVcf;
    }

    public BcfToolsCommandBuilder includeHardPass() {
        return includeHardFilter("'FILTER=\"PASS\"'");
    }

    public BcfToolsCommandBuilder includeHardFilter(String expression) {
        addArguments("filter", "-i", expression);
        return this;
    }

    public BcfToolsCommandBuilder excludeSoftFilter(String expression, String softFilter) {
        addArguments("filter", "-e", expression, "-s", softFilter, "-m+");
        return this;
    }

    public BcfToolsCommandBuilder removeAnnotation(String annotation) {
        addArguments("annotate", "-x", annotation);
        return this;
    }

    public BcfToolsCommandBuilder addAnnotation(String file, String annotation) {
        addArguments("annotate", "-a", file, "-c", annotation);
        return this;
    }

    public BcfToolsCommandBuilder addAnnotation(String file, String annotation, String header) {
        addArguments("annotate", "-a", file, "-h", header, "-c", annotation);
        return this;
    }

    public BcfToolsCommandBuilder selectSample(String tumorSampleName) {
        addArguments("view", "-s", tumorSampleName);
        return this;
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

    public BashCommand build() {
        if (currentArguments == null) {
            throw new IllegalStateException("No bcftools command added.");
        }

        final List<BashCommand> finalCommands = Lists.newArrayList(pipedCommands);
        final BashCommand finalCommand = new BcfToolsCommand(currentArguments, "-O", "z", "-o", outputVcf);
        finalCommands.add(finalCommand);

        return new PipeCommands(finalCommands.toArray(new BashCommand[finalCommands.size()]));
    }

    public List<BashCommand> buildAndIndex() {
        return ImmutableList.of(build(), new TabixCommand(outputVcf));
    }
}
