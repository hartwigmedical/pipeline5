package com.hartwig.pipeline.calling.sage;

import java.util.List;
import java.util.StringJoiner;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.resource.ResourceFiles;

public class SageCommandBuilder {

    private final ResourceFiles resourceFiles;
    private final StringJoiner tumor = new StringJoiner(",");
    private final StringJoiner reference = new StringJoiner(",");
    private final List<String> tumorBam = Lists.newArrayList();
    private final List<String> referenceBam = Lists.newArrayList();

    private String maxHeap = "60G";
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
        panelOnly = true;
        germlineMode = true;
        somaticMode = false;
        panelOnly();
        maxHeap("15G");
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
        this.maxHeap = maxHeap;
        return this;
    }

    private List<BashCommand> sliceAndConvertToBam(String oldFile, String newFile) {
        List<BashCommand> result = Lists.newArrayList();
        result.add(SamtoolsCommand.sliceToUncompressedBam(resourceFiles, resourceFiles.sageGermlineSlicePanel(), oldFile, newFile));
        result.add(SamtoolsCommand.index(newFile));
        return result;
    }

    private List<BashCommand> convertToBam(String oldFile, String newFile) {
        List<BashCommand> result = Lists.newArrayList();
        result.add(SamtoolsCommand.toUncompressedBam(resourceFiles, oldFile, newFile));
        result.add(SamtoolsCommand.index(newFile));
        return result;
    }

    public List<BashCommand> build(final String outputVcf) {
        List<BashCommand> result = Lists.newArrayList();

        for (int i = 0; i < referenceBam.size(); i++) {
            String currentAlignmentFile = referenceBam.get(i);
            if (currentAlignmentFile.endsWith(".cram")) {
                String newBamFile = currentAlignmentFile.substring(0, currentAlignmentFile.length() - 5) + ".bam";
                if (germlineMode && panelOnly) {
                    result.addAll(sliceAndConvertToBam(currentAlignmentFile, newBamFile));
                } else {
                    result.addAll(convertToBam(currentAlignmentFile, newBamFile));
                }
                referenceBam.set(i, newBamFile);
            }
        }

        for (int i = 0; i < tumorBam.size(); i++) {
            String currentAlignmentFile = tumorBam.get(i);
            if (currentAlignmentFile.endsWith(".cram")) {
                String newBamFile = currentAlignmentFile.substring(0, currentAlignmentFile.length() - 5) + ".bam";
                if (germlineMode && panelOnly) {
                    result.addAll(sliceAndConvertToBam(currentAlignmentFile, newBamFile));
                } else {
                    result.addAll(convertToBam(currentAlignmentFile, newBamFile));
                }
                tumorBam.set(i, newBamFile);
            }
        }

        result.add(buildSageCommand(outputVcf));
        return result;
    }

    private SageCommand buildSageCommand(String outputVcf) {
        final StringJoiner arguments = new StringJoiner(" ");

        if (tumorSamples == 0) {
            throw new IllegalStateException("Must be at least one tumor");
        }

        if (shallowSomaticMode && !somaticMode) {
            throw new IllegalStateException("Shallow somatic mode enabled while not in shallow mode");
        }

        if (somaticMode) {
            final String tumorBamFiles = String.join(",", tumorBam);
            arguments.add("-tumor").add(tumor.toString()).add("-tumor_bam").add(tumorBamFiles);

            if (reference.length() > 0) {
                final String referenceBamFiles = String.join(",", referenceBam);
                arguments.add("-reference").add(reference.toString()).add("-reference_bam").add(referenceBamFiles);
            }

            arguments.add("-hotspots").add(resourceFiles.sageSomaticHotspots());
            arguments.add("-panel_bed").add(resourceFiles.sageSomaticCodingPanel());

            if (shallowSomaticMode) {
                arguments.add("-hotspot_min_tumor_qual").add("40");
            }

        } else if (germlineMode) {

            final String referenceBamFiles = String.join(",", referenceBam);
            arguments.add("-tumor").add(reference.toString()).add("-tumor_bam").add(referenceBamFiles);

            if (tumor.length() > 0) {
                final String tumorBamFiles = String.join(",", tumorBam);
                arguments.add("-reference").add(tumor.toString()).add("-reference_bam").add(tumorBamFiles);
            }

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

        if (panelOnly) {
            arguments.add("-panel_only");
        }

        if (coverage) {
            arguments.add("-coverage_bed");
            if (germlineMode) {
                arguments.add(resourceFiles.sageGermlineCoveragePanel());
            } else {
                arguments.add(resourceFiles.sageSomaticCodingPanel());
            }
        }

        arguments.add("-high_confidence_bed").add(resourceFiles.giabHighConfidenceBed());
        arguments.add("-ref_genome").add(resourceFiles.refGenomeFile());
        arguments.add("-ref_genome_version").add(resourceFiles.version().toString());
        arguments.add("-ensembl_data_dir").add(resourceFiles.ensemblDataCache());
        arguments.add("-write_bqr_data ");
        arguments.add("-write_bqr_plot ");
        arguments.add("-out").add(outputVcf);
        arguments.add("-threads").add(Bash.allCpus());

        return new SageCommand("com.hartwig.hmftools.sage.SageApplication", maxHeap, arguments.toString());
    }

}
