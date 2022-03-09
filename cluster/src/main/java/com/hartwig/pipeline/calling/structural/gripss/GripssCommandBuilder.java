package com.hartwig.pipeline.calling.structural.gripss;

import java.util.Optional;

import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tertiary.HmfToolCommandBuilder;
import com.hartwig.pipeline.tools.Versions;

public class GripssCommandBuilder extends HmfToolCommandBuilder {

    private GripssCommandBuilder(final ResourceFiles resourceFiles, final String outputId) {
        super("gripss", Versions.GRIPSS, "16G", "gripss.jar");
        addArguments("-ref_genome",
                resourceFiles.refGenomeFile(),
                "-known_hotspot_file",
                resourceFiles.knownFusionPairBedpe(),
                "-pon_sgl_file",
                resourceFiles.gridssBreakendPon(),
                "-pon_sv_file",
                resourceFiles.gridssBreakpointPon(),
                "-output_id",
                outputId);
    }

    public static GripssCommandBuilder newBuilder(final ResourceFiles resourceFiles, final String outputId) {
        return new GripssCommandBuilder(resourceFiles, outputId);
    }

    public GripssCommandBuilder inputVcf(final String inputVcfLocation) {
        addArguments("-vcf", inputVcfLocation);
        return this;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public GripssCommandBuilder maybeReference(final Optional<String> maybeReferenceSample) {
        maybeReferenceSample.ifPresent(referenceSample -> addArguments("-reference", referenceSample));
        return this;
    }

    public GripssCommandBuilder sample(final String sample) {
        addArguments("-sample", sample);
        return this;
    }
}
