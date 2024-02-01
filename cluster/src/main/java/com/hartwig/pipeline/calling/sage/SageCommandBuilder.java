package com.hartwig.pipeline.calling.sage;

import com.google.api.client.util.Lists;
import com.hartwig.computeengine.execution.vm.Bash;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.execution.JavaCommandFactory;
import com.hartwig.pipeline.resource.ResourceFiles;

import java.util.List;
import java.util.StringJoiner;

import static com.hartwig.pipeline.tools.HmfTool.SAGE;

public class SageCommandBuilder {

    private final ResourceFiles resourceFiles;
    private final StringJoiner tumor = new StringJoiner(",");
    private final StringJoiner reference = new StringJoiner(",");
    private final List<String> tumorBam = Lists.newArrayList();
    private final List<String> referenceBam = Lists.newArrayList();

    private boolean somaticMode = true;
    private boolean germlineMode = false;
    private boolean shallowSomaticMode = false;
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

    public SageCommandBuilder shallowMode(final boolean enabled) {
        this.shallowSomaticMode = enabled;
        return this;
    }

    public SageCommandBuilder targetRegionsMode(final boolean enabled) {
        this.targetRegions = enabled;
        return this;
    }

    public List<BashCommand> build(final String outputVcf) {
        List<BashCommand> result = Lists.newArrayList();

        if (shallowSomaticMode && !somaticMode) {
            throw new IllegalStateException("Shallow somatic mode enabled while not in shallow mode");
        }

        if (tumorBam.isEmpty() && referenceBam.isEmpty()) {
            throw new IllegalStateException("Must be at least one tumor or reference");
        }

        final List<String> arguments = Lists.newArrayList();

        final String tumorBamFiles = tumor.length() > 0 ? String.join(",", tumorBam) : "";
        final String referenceBamFiles = reference.length() > 0 ? String.join(",", referenceBam) : "";

        if (somaticMode) {

            arguments.add(String.format("-tumor %s", tumor));
            arguments.add(String.format("-tumor_bam %s", tumorBamFiles));

            if (reference.length() > 0) {

                arguments.add(String.format("-reference %s", reference));
                arguments.add(String.format("-reference_bam %s", referenceBamFiles));
            }

            arguments.add(String.format("-hotspots %s", resourceFiles.sageSomaticHotspots()));

            if (shallowSomaticMode) {
                arguments.add("-hotspot_min_tumor_qual 40");
            }

        } else if (germlineMode) {

            arguments.add(String.format("-tumor %s", reference));
            arguments.add(String.format("-tumor_bam %s", referenceBamFiles));

            if (tumor.length() > 0) {
                arguments.add(String.format("-reference %s", tumor));
                arguments.add(String.format("-reference_bam %s", tumorBamFiles));
            }

            arguments.add(String.format("-hotspots %s", resourceFiles.sageGermlineHotspots()));
            arguments.add("-hotspot_min_tumor_qual 50");
            arguments.add("-panel_min_tumor_qual 75");
            arguments.add("-hotspot_max_germline_vaf 100");
            arguments.add("-hotspot_max_germline_rel_raw_base_qual 100");
            arguments.add("-panel_max_germline_vaf 100");
            arguments.add("-panel_max_germline_rel_raw_base_qual 100");
            arguments.add("-panel_only");
            arguments.add("-ref_sample_count 0");
        }

        if (targetRegions) {
            arguments.add("-high_depth_mode");
            arguments.add("-hard_min_tumor_vaf 0.002");
            arguments.add("-hotspot_min_tumor_vaf 0.015");
            arguments.add("-panel_min_tumor_qual 150");
        }

        arguments.add(String.format("-high_confidence_bed %s", resourceFiles.giabHighConfidenceBed()));
        arguments.add(String.format("-panel_bed %s", resourceFiles.sagePanelBed()));
        arguments.add(String.format("-coverage_bed %s", resourceFiles.sageGeneCoverageBed()));
        arguments.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
        arguments.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        arguments.add("-write_bqr_data");
        arguments.add("-write_bqr_plot");
        arguments.add(String.format("-output_vcf %s", outputVcf));
        arguments.add(String.format("-threads %s", Bash.allCpus()));

        result.add(() -> "eval `/root/anaconda3/bin/conda shell.bash hook`");
        result.add(() -> "source /root/anaconda3/bin/activate");
        result.add(() -> "conda activate /root/anaconda3/envs/bioconductor-r42");
        result.add(JavaCommandFactory.javaJarCommand(SAGE, arguments));

        return result;
    }
}
