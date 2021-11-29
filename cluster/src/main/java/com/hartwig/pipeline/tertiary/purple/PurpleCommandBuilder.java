package com.hartwig.pipeline.tertiary.purple;

import static com.google.common.collect.Lists.newArrayList;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;

import org.jetbrains.annotations.NotNull;

public class PurpleCommandBuilder {

    private static final String LOW_COVERAGE_DIPLOID_PERCENTAGE = "0.88";
    private static final String LOW_COVERAGE_SOMATIC_MIN_PURITY_SPREAD = "0.1";
    private static final String CIRCOS_PATH = VmDirectories.TOOLS + "/circos/" + Versions.CIRCOS + "/bin/circos";

    private final ResourceFiles resourceFiles;
    private final String amberDirectory;
    private final String cobaltDirectory;
    private final String tumorSampleName;
    private final String structuralVcf;
    private final String svRecoveryVcf;
    private final String somaticVcf;

    private String referenceSampleName;
    private String germlineVcf;
    private boolean isShallow;

    public PurpleCommandBuilder(final ResourceFiles resourceFiles, final String amberDirectory, final String cobaltDirectory,
            final String tumorSample, final String structuralVcf, final String svRecoveryVcf, final String somaticVcf) {
        this.resourceFiles = resourceFiles;
        this.amberDirectory = amberDirectory;
        this.cobaltDirectory = cobaltDirectory;
        this.tumorSampleName = tumorSample;
        this.structuralVcf = structuralVcf;
        this.svRecoveryVcf = svRecoveryVcf;
        this.somaticVcf = somaticVcf;
    }

    public PurpleCommandBuilder setShallow(boolean isShallow) {
        this.isShallow = isShallow;
        return this;
    }

    public PurpleCommandBuilder addGermline(String germlineVcf) {
        this.germlineVcf = germlineVcf;
        return this;
    }

    public PurpleCommandBuilder setReferenceSample(String referenceSample){
        this.referenceSampleName = referenceSample;
        return this;
    }

    private boolean isTumorOnly() {
        return referenceSampleName == null;
    }

    public BashCommand build() {
        final List<String> arguments = Lists.newArrayList();
        if (isTumorOnly()) {
            arguments.add("-tumor_only");
        } else {
            arguments.add("-reference");
            arguments.add(referenceSampleName);
            if (germlineVcf != null && !germlineVcf.isEmpty()) {
                arguments.add("-germline_vcf");
                arguments.add(germlineVcf);
                arguments.add("-germline_hotspots");
                arguments.add(resourceFiles.sageGermlineHotspots());
            }
        }

        arguments.addAll(commonArguments());

        if (isShallow) {
            arguments.addAll(shallowArguments());
        }

        return new JavaJarCommand("purple", Versions.PURPLE, "purple.jar", "12G", arguments);
    }

    @NotNull
    private List<String> commonArguments() {
        return newArrayList("-tumor",
                tumorSampleName,
                "-output_dir",
                VmDirectories.OUTPUT,
                "-amber",
                amberDirectory,
                "-cobalt",
                cobaltDirectory,
                "-gc_profile",
                resourceFiles.gcProfileFile(),
                "-somatic_vcf",
                somaticVcf,
                "-structural_vcf",
                structuralVcf,
                "-sv_recovery_vcf",
                svRecoveryVcf,
                "-circos",
                CIRCOS_PATH,
                "-ref_genome",
                resourceFiles.refGenomeFile(),
                "-ensembl_data_dir",
                resourceFiles.ensemblDataCache(),
                "-run_drivers",
                "-somatic_hotspots",
                resourceFiles.sageSomaticHotspots(),
                "-driver_gene_panel",
                resourceFiles.driverGenePanel(),
                "-threads",
                Bash.allCpus());
    }

    private static List<String> shallowArguments() {
        return newArrayList("-highly_diploid_percentage",
                LOW_COVERAGE_DIPLOID_PERCENTAGE,
                "-somatic_min_purity_spread",
                LOW_COVERAGE_SOMATIC_MIN_PURITY_SPREAD);
    }

}

