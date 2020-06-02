package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;

public class SageV2Application extends SubStage {

    private final SageCommandBuilder sageCommandBuilder;

    public SageV2Application(final ResourceFiles resourceFiles) {
        super("sage.somatic", OutputFile.GZIPPED_VCF);
        sageCommandBuilder = new SageCommandBuilder(resourceFiles);
    }

    public SageV2Application(final ResourceFiles resourceFiles, final String tumorBamPath, final String referenceBamPath,
            final String tumorSampleName, final String referenceSampleName) {
        this(resourceFiles);
        sageCommandBuilder.addReference(referenceSampleName, referenceBamPath).addTumor(tumorSampleName, tumorBamPath);
    }

    public void panelOnly() {
        sageCommandBuilder.panelOnly();
    }

    public SageV2Application germlineMode(String sample, String bamFile) {
        sageCommandBuilder.addTumor(sample, bamFile).germlineMode();
        return this;
    }

    public void addTumor(String sample, String bamFile) {
        sageCommandBuilder.addTumor(sample, bamFile);
    }

    public void addReference(String sample, String bamFile) {
        sageCommandBuilder.addReference(sample, bamFile);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return ImmutableList.of(sageCommandBuilder.build(output.path()));
    }
}
