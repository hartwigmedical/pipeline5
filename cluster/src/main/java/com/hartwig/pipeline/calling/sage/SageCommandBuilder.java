package com.hartwig.pipeline.calling.sage;

import java.util.List;
import java.util.StringJoiner;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;

public class SageCommandBuilder {

    private final ResourceFiles resourceFiles;
    private final StringJoiner tumor = new StringJoiner(",");
    private final StringJoiner reference = new StringJoiner(",");
    private final List<String> tumorBam = Lists.newArrayList();
    private final List<String> referenceBam = Lists.newArrayList();

    private String wgsMaxHeap = "60G";
    private String panelMaxHeap = "15G";

    private boolean coverage = false;
    private boolean panelOnly = false;
    private boolean somaticMode = true;
    private boolean germlineMode = false;
    private boolean shallowSomaticMode = false;
    private int tumorSamples = 0;

    public SageCommandBuilder(ResourceFiles resourceFiles) {
        this.resourceFiles = resourceFiles;
    }

    public boolean isSomatic() {
        return somaticMode;
    }

    public SageCommandBuilder panelOnly() {
        panelOnly = true;
        return this;
    }

    public SageCommandBuilder germlineMode() {
        germlineMode = true;
        somaticMode = false;
        maxHeap(panelMaxHeap);
        return this;
    }

    public SageCommandBuilder addTumor(String sample, String bamFile) {
        tumorSamples++;
        tumor.add(sample);
        tumorBam.add(bamFile);
        return this;
    }

    public SageCommandBuilder addReference(String sample, String bamFile) {
        reference.add(sample);
        referenceBam.add(bamFile);
        return this;
    }

    public SageCommandBuilder addCoverage() {
        this.coverage = true;
        return this;
    }

    public SageCommandBuilder shallowMode(final boolean enabled) {
        this.shallowSomaticMode = enabled;
        return this;
    }

    public SageCommandBuilder maxHeap(String maxHeap) {
        this.wgsMaxHeap = maxHeap;
        return this;
    }

    public List<BashCommand> build(final String outputVcf) {
        List<BashCommand> result = Lists.newArrayList();

        final List<String> arguments = Lists.newArrayList();

        if (tumorSamples == 0) {
            throw new IllegalStateException("Must be at least one tumor");
        }

        if (shallowSomaticMode && !somaticMode) {
            throw new IllegalStateException("Shallow somatic mode enabled while not in shallow mode");
        }

        final String tumorBamFiles = tumor.length() > 0 ? String.join(",", tumorBam) : "";
        final String referenceBamFiles = reference.length() > 0 ? String.join(",", referenceBam) : "";

        if (somaticMode) {

            arguments.add(String.format("-tumor %s", tumor.toString()));
            arguments.add(String.format("-tumor_bam %s", tumorBamFiles));

            if (reference.length() > 0) {

                arguments.add(String.format("-reference %s", reference.toString()));
                arguments.add(String.format("-reference_bam %s", referenceBamFiles));
            }

            arguments.add(String.format("-hotspots %s", resourceFiles.sageSomaticHotspots()));
            arguments.add(String.format("-panel_bed %s", resourceFiles.sageSomaticCodingPanel()));

            if (shallowSomaticMode) {
                arguments.add("-hotspot_min_tumor_qual 40");
            }

        } else if (germlineMode) {

            arguments.add(String.format("-tumor %s", reference.toString()));
            arguments.add(String.format("-tumor_bam %s", referenceBamFiles));

            if (tumor.length() > 0) {
                arguments.add(String.format("-reference %s", tumor.toString()));
                arguments.add(String.format("-reference_bam %s", tumorBamFiles));
            }

            arguments.add(String.format("-hotspots %s", resourceFiles.sageGermlineHotspots()));
            arguments.add(String.format("-panel_bed %s", resourceFiles.sageGermlineCodingPanel()));
            arguments.add("-hotspot_min_tumor_qual 50");
            arguments.add("-panel_min_tumor_qual 75");
            arguments.add("-hotspot_max_germline_vaf 100");
            arguments.add("-hotspot_max_germline_rel_raw_base_qual 100");
            arguments.add("-panel_max_germline_vaf 100");
            arguments.add("-panel_max_germline_rel_raw_base_qual 100");
            arguments.add("-mnv_filter_enabled false");
            arguments.add("-panel_only");
        }

        if (coverage) {
            if (germlineMode) {
                arguments.add(String.format("-coverage_bed %s", resourceFiles.sageGermlineCoveragePanel()));
            } else {
                arguments.add(String.format("-coverage_bed %s", resourceFiles.sageSomaticCodingPanel()));
            }
        }

        if(panelOnly)
        {
            arguments.add("-hotspot_min_tumor_qual 100");
            arguments.add("-panel_min_tumor_qual 200");
            arguments.add("-high_confidence_min_tumor_qual 200");
            arguments.add("-low_confidence_min_tumor_qual 300");
        }

        arguments.add(String.format("-high_confidence_bed %s", resourceFiles.giabHighConfidenceBed()));
        arguments.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
        arguments.add(String.format("-ensembl_data_dir %s", resourceFiles.ensemblDataCache()));
        arguments.add("-write_bqr_data");
        arguments.add("-write_bqr_plot");
        arguments.add(String.format("-out %s", outputVcf));
        arguments.add(String.format("-threads %s", Bash.allCpus()));

        result.add(new JavaJarCommand("sage", Versions.SAGE, "sage.jar", wgsMaxHeap, arguments));

        return result;
    }
}
