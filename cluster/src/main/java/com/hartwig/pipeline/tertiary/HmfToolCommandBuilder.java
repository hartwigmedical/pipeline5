package com.hartwig.pipeline.tertiary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;

public class HmfToolCommandBuilder {

    private final String tool;
    private final String version;
    private final String heap;
    private final String jar;
    private final List<String> arguments;

    public HmfToolCommandBuilder(final String tool, final String version, final String heap, final String jar) {
        this.tool = tool;
        this.version = version;
        this.heap = heap;
        this.jar = jar;
        arguments = new ArrayList<>();
    }

    public HmfToolCommandBuilder tumor(final String tumorSample, final String tumorBamPath) {
        arguments.addAll(List.of("-tumor", tumorSample, "-tumor_bam", tumorBamPath));
        return this;
    }

    public HmfToolCommandBuilder reference(final String referenceSample, final String referenceBamPath) {
        arguments.addAll(List.of("-reference", referenceSample, "-reference_bam", referenceBamPath));
        return this;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public HmfToolCommandBuilder targetRegionsBed(final Optional<String> targetRegionsBedLocation) {
        //  arguments.addAll(targetRegionsBedLocation.stream().flatMap(l -> Stream.of("-panel_bed", l)).collect(Collectors.toList()));
        return this;
    }

    public HmfToolCommandBuilder addArguments(final String... args) {
        arguments.addAll(Arrays.asList(args));
        return this;
    }

    public BashCommand build() {
        arguments.add("-output_dir");
        arguments.add(VmDirectories.OUTPUT);
        return new JavaJarCommand(tool, version, jar, heap, arguments);
    }

}
