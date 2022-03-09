package com.hartwig.pipeline.calling.structural.gridss.stage;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.tools.Versions;

import org.jetbrains.annotations.NotNull;

public class Driver extends SubStage {

    private static final String GRIDSS = "gridss";
    private final ResourceFiles resourceFiles;
    private final String assemblyBamPath;
    private final SortedSet<SampleArgument> sampleArguments = new TreeSet<>();

    public Driver(final ResourceFiles resourceFiles, final String assemblyBamPath) {
        super("gridss.driver", FileTypes.GZIPPED_VCF);
        this.resourceFiles = resourceFiles;
        this.assemblyBamPath = assemblyBamPath;
    }

    public Driver tumorSample(final String tumorSampleName, final String tumorBamPath) {
        sampleArguments.add(new SampleArgument("tumor", tumorSampleName, tumorBamPath));
        return this;
    }

    public Driver referenceSample(final String referenceSampleName, final String referenceSamplePath) {
        sampleArguments.add(new SampleArgument("reference", referenceSampleName, referenceSamplePath));
        return this;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return List.of(new VersionedToolCommand(GRIDSS,
                "gridss",
                Versions.GRIDSS,
                Stream.concat(Stream.of("--output",
                        output.path(),
                        "--assembly",
                        assemblyBamPath,
                        "--workingdir",
                        VmDirectories.OUTPUT,
                        "--reference",
                        resourceFiles.refGenomeFile(),
                        "--jar",
                        GridssJar.path(),
                        "--blacklist",
                        resourceFiles.gridssBlacklistBed(),
                        "--configuration",
                        resourceFiles.gridssPropertiesFile(),
                        "--labels",
                        sampleArguments.stream().map(SampleArgument::getSampleName).collect(Collectors.joining(",")),
                        "--jvmheap",
                        "31G",
                        "--externalaligner"), sampleArguments.stream().map(SampleArgument::getSamplePath)).collect(Collectors.toList())));
    }

    private static class SampleArgument implements Comparable<SampleArgument> {
        private final String type;
        private final String sampleName;
        private final String samplePath;

        private SampleArgument(final String type, final String sampleName, final String samplePath) {
            this.type = type;
            this.sampleName = sampleName;
            this.samplePath = samplePath;
        }

        public String getSampleName() {
            return sampleName;
        }

        public String getSamplePath() {
            return samplePath;
        }

        @Override
        public int compareTo(@NotNull final Driver.SampleArgument o) {
            return referenceFirst(o);
        }

        private int referenceFirst(final SampleArgument o) {
            return this.type.compareTo(o.type);
        }
    }
}
