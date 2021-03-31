package com.hartwig.pipeline.calling.sage;

import java.util.StringJoiner;

import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.resource.ResourceFiles;

public class SageCommandBuilder {

    private final ResourceFiles resourceFiles;
    private final StringJoiner tumor = new StringJoiner(",");
    private final StringJoiner tumorBam = new StringJoiner(",");
    private final StringJoiner reference = new StringJoiner(",");
    private final StringJoiner referenceBam = new StringJoiner(",");

    private String coverageBed = "";
    private String maxHeap = "110G";
    private boolean panelOnly = false;
    private boolean ponMode = false;
    private boolean somaticMode = true;
    private boolean germlineMode = false;
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

    public SageCommandBuilder germlineMode(String referenceSample, String referenceBam, String tumorSample, String tumorBam) {
        panelOnly = true;
        germlineMode = true;
        somaticMode = false;
        // Note that we are adding the reference sample as the tumor
        addTumor(referenceSample, referenceBam);
        addReference(tumorSample, tumorBam);
        panelOnly();
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

    public SageCommandBuilder addCoverage(String coverageBed) {
        this.coverageBed = coverageBed;
        return this;
    }

    public SageCommandBuilder ponMode(String sample, String bamFile) {
        ponMode = true;
        return addTumor(sample, bamFile);
    }

    public SageCommandBuilder maxHeap(String maxHeap) {
        this.maxHeap = maxHeap;
        return this;
    }

    public SageCommand build(String outputVcf) {
        final StringJoiner arguments = new StringJoiner(" ");

        if (tumorSamples == 0) {
            throw new IllegalStateException("Must be at least one tumor");
        }

        arguments.add("-tumor").add(tumor.toString()).add("-tumor_bam").add(tumorBam.toString());
        if (reference.length() > 0) {
            arguments.add("-reference").add(reference.toString()).add("-reference_bam").add(referenceBam.toString());
        }

        if (somaticMode) {
            arguments.add("-hotspots").add(resourceFiles.sageSomaticHotspots());
            arguments.add("-panel_bed").add(resourceFiles.sageSomaticCodingPanel());
        }

        if (germlineMode) {
            arguments.add("-hotspots").add(resourceFiles.sageGermlineHotspots());
            arguments.add("-panel_bed").add(resourceFiles.sageGermlineCodingPanel());
            arguments.add("-hotspot_min_tumor_qual").add("50");
            arguments.add("-panel_min_tumor_qual").add("75");
            arguments.add("-hotspot_max_germline_vaf").add("100");
            arguments.add("-hotspot_max_germline_rel_raw_base_qual").add("100");
            arguments.add("-panel_max_germline_vaf").add("100");
            arguments.add("-panel_max_germline_rel_raw_base_qual").add("100");
            arguments.add("-mnv_filter_enabled").add("false");
        }

        arguments.add("-high_confidence_bed").add(resourceFiles.giabHighConfidenceBed());
        arguments.add("-ref_genome").add(resourceFiles.refGenomeFile());
        arguments.add("-out").add(outputVcf);
        arguments.add("-assembly").add(resourceFiles.version().sage());
        arguments.add("-threads").add(Bash.allCpus());

        if (panelOnly) {
            arguments.add("-panel_only");
        }

        if (!coverageBed.isEmpty()) {
            arguments.add("-coverage_bed").add(coverageBed);
        }

        if (ponMode) {

            arguments.add("-hotspots").add(resourceFiles.sageSomaticHotspots());
            arguments.add("-panel_bed").add(resourceFiles.sageSomaticCodingPanel());

            if (tumorSamples > 1) {
                throw new IllegalStateException("PON mode only supports one sample");
            }

            arguments.add("-hard_filter_enabled true")
                    .add("-soft_filter_enabled false")
                    .add("-hard_min_tumor_qual 0")
                    .add("-hard_min_tumor_raw_alt_support 3")
                    .add("-hard_min_tumor_raw_base_quality 30");
        }

        return new SageCommand("com.hartwig.hmftools.sage.SageApplication", maxHeap, arguments.toString());
    }

}
