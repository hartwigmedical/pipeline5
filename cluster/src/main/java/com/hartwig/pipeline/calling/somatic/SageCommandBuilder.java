package com.hartwig.pipeline.calling.somatic;

import java.util.StringJoiner;

import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.resource.ResourceFiles;

public class SageCommandBuilder {

    private final ResourceFiles resourceFiles;
    private boolean panelOnly = false;
    private StringJoiner tumor = new StringJoiner(",");
    private StringJoiner tumorBam = new StringJoiner(",");
    private StringJoiner reference = new StringJoiner(",");
    private StringJoiner referenceBam = new StringJoiner(",");

    public SageCommandBuilder(ResourceFiles resourceFiles) {
        this.resourceFiles = resourceFiles;
    }

    public SageCommandBuilder panelOnly() {
        panelOnly = true;
        return this;
    }

    public SageCommandBuilder addTumor(String sample, String bamFile) {
        tumor.add(sample);
        tumorBam.add(bamFile);
        return this;
    }

    public SageCommandBuilder addReference(String sample, String bamFile) {
        reference.add(sample);
        referenceBam.add(bamFile);
        return this;
    }

    public SageCommand build(String outputVcf) {
        final StringJoiner arguments = new StringJoiner(" ");

        if (tumor.length() == 0) {
            throw new IllegalStateException("Must be at least one tumor");
        }

        arguments.add("-tumor").add(tumor.toString()).add("-tumor_bam").add(tumorBam.toString());
        if (reference.length() > 0) {
            arguments.add("-reference").add(reference.toString()).add("-reference_bam").add(referenceBam.toString());
        }

        arguments.add("-hotspots").add(resourceFiles.sageKnownHotspots());
        arguments.add("-panel_bed").add(resourceFiles.sageActionableCodingPanel());
        arguments.add("-high_confidence_bed").add(resourceFiles.giabHighConfidenceBed());
        arguments.add("-ref_genome").add(resourceFiles.refGenomeFile());
        arguments.add("-out").add(outputVcf);
        arguments.add("-threads").add(Bash.allCpus());

        if (panelOnly) {
            arguments.add("-panel_only");
        }

        return new SageCommand("com.hartwig.hmftools.sage.SageApplication", "110G", arguments.toString());
    }

}
