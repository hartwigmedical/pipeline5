package com.hartwig.pipeline.calling.sage;

import static java.lang.String.format;

import static com.hartwig.pipeline.tools.HmfTool.SAGE;

import java.util.List;
import java.util.StringJoiner;

import com.google.api.client.util.Lists;
import com.hartwig.computeengine.execution.vm.Bash;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.execution.JavaCommandFactory;
import com.hartwig.pipeline.resource.ResourceFiles;

public class SageCommandBuilder {

    private final ResourceFiles resourceFiles;
    private final StringJoiner tumor = new StringJoiner(",");
    private final StringJoiner reference = new StringJoiner(",");
    private final List<String> tumorBam = Lists.newArrayList();
    private final List<String> referenceBam = Lists.newArrayList();

    private boolean somaticMode = true;
    private boolean germlineMode = false;
    private boolean targetRegions = false;

    public SageCommandBuilder(final ResourceFiles resourceFiles) {
        this.resourceFiles = resourceFiles;
    }

    public boolean isSomatic() {
        return somaticMode;
    }

    public SageCommandBuilder germlineMode() {
        germlineMode = true;
        somaticMode = false;
        return this;
    }

    public SageCommandBuilder addTumor(final String sample, final String bamFile) {
        tumor.add(sample);
        tumorBam.add(bamFile);
        return this;
    }

    public SageCommandBuilder addReference(final String sample, final String bamFile) {
        reference.add(sample);
        referenceBam.add(bamFile);
        return this;
    }

    public SageCommandBuilder targetRegionsMode(final boolean enabled) {
        this.targetRegions = enabled;
        return this;
    }

    public List<BashCommand> build(final String outputVcf) {
        List<BashCommand> result = Lists.newArrayList();

        if (tumorBam.isEmpty() && referenceBam.isEmpty()) {
            throw new IllegalStateException("Must be at least one tumor or reference");
        }

        final List<String> arguments = Lists.newArrayList();

        final String tumorBamFiles = tumor.length() > 0 ? String.join(",", tumorBam) : "";
        final String referenceBamFiles = reference.length() > 0 ? String.join(",", referenceBam) : "";

        if (somaticMode) {

            arguments.add(format("-tumor %s", tumor));
            arguments.add(format("-tumor_bam %s", tumorBamFiles));

            if (reference.length() > 0) {

                arguments.add(format("-reference %s", reference));
                arguments.add(format("-reference_bam %s", referenceBamFiles));
            }

            arguments.add(format("-hotspots %s", resourceFiles.sageSomaticHotspots()));
        } else if (germlineMode) {

            arguments.add(format("-tumor %s", reference));
            arguments.add(format("-tumor_bam %s", referenceBamFiles));

            if (tumor.length() > 0) {
                arguments.add(format("-reference %s", tumor));
                arguments.add(format("-reference_bam %s", tumorBamFiles));
            }

            arguments.add(format("-hotspots %s", resourceFiles.sageGermlineHotspots()));

            arguments.add("-germline");
            arguments.add("-panel_only");
            arguments.add("-ref_sample_count 0");
        }

        if (targetRegions) {
            arguments.add("-high_depth_mode");
        }

        arguments.add(format("-jitter_param_dir %s/", VmDirectories.INPUT));
        arguments.add(format("-high_confidence_bed %s", resourceFiles.giabHighConfidenceBed()));
        arguments.add(format("-panel_bed %s", resourceFiles.sagePanelBed()));
        arguments.add(format("-coverage_bed %s", resourceFiles.sageGeneCoverageBed()));
        arguments.add(format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(format("-ref_genome_version %s", resourceFiles.version().toString()));
        arguments.add(format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        arguments.add(format("-output_vcf %s", outputVcf));
        arguments.add("-bqr_write_plot");
        arguments.add(format("-threads %s", Bash.allCpus()));

        // add for regression testing
        // arguments.add("-log_debug");

        result.add(JavaCommandFactory.javaJarCommand(SAGE, arguments));

        return result;
    }
}
