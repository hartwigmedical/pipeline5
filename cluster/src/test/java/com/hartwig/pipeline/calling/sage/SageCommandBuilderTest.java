package com.hartwig.pipeline.calling.sage;

import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.tools.HmfTool.SAGE;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class SageCommandBuilderTest {

    private static final String TUMOR = "COLO829v003T";
    private static final String TUMOR_BAM = VmDirectories.INPUT + "/" + TUMOR + ".bam";
    private static final String REFERENCE = "COLO829v003R";
    private static final String REFERENCE_BAM = VmDirectories.INPUT + "/" + REFERENCE + ".bam";

    private static final String REFERENCE_OUT = VmDirectories.OUTPUT + "/" + REFERENCE + ".out.vcf.gz";
    private static final String REFERENCE_SAGE_COMMAND =
            toolCommand(SAGE)
                    + " -tumor COLO829v003R -tumor_bam /data/input/COLO829v003R.bam "
                    + "-reference COLO829v003T -reference_bam /data/input/COLO829v003T.bam "
                    + "-hotspots /opt/resources/sage/37/KnownHotspots.germline.37.vcf.gz "
                    + "-hotspot_min_tumor_qual 50 -panel_min_tumor_qual 75 -hotspot_max_germline_vaf 100 -hotspot_max_germline_rel_raw_base_qual 100 "
                    + "-panel_max_germline_vaf 100 -panel_max_germline_rel_raw_base_qual 100 " + "-panel_only -ref_sample_count 0 "
                    + "-high_confidence_bed /opt/resources/giab_high_conf/37/NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz "
                    + "-panel_bed /opt/resources/sage/37/ActionableCodingPanel.37.bed.gz "
                    + "-coverage_bed /opt/resources/sage/37/CoverageCodingPanel.37.bed.gz "
                    + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta " + "-ref_genome_version V37 "
                    + "-ensembl_data_dir /opt/resources/ensembl_data_cache/37/ " + "-write_bqr_data -write_bqr_plot "
                    + "-out /data/output/COLO829v003R.out.vcf.gz " + "-threads $(grep -c '^processor' /proc/cpuinfo)";
    public static final String EMPTY = "";

    @Test
    public void runsOnGermlineBam() {
        SageCommandBuilder victim = new SageCommandBuilder(TestInputs.REF_GENOME_37_RESOURCE_FILES);
        victim.germlineMode().addReference(REFERENCE, REFERENCE_BAM).addTumor(TUMOR, TUMOR_BAM);
        List<String> bash = victim.build(REFERENCE_OUT).stream().map(BashCommand::asBash).collect(Collectors.toList());
        assertEquals(1, bash.size());
        assertEquals(REFERENCE_SAGE_COMMAND, bash.get(0));
    }

    @Test(expected = IllegalStateException.class)
    public void throwsExceptionOnNoTumorSet() {
        new SageCommandBuilder(TestInputs.REF_GENOME_37_RESOURCE_FILES).build(EMPTY);
    }

    @Test(expected = IllegalStateException.class)
    public void throwsExceptionOnShallowModeInGermline() {
        new SageCommandBuilder(TestInputs.REF_GENOME_37_RESOURCE_FILES).germlineMode().shallowMode(true).build(EMPTY);
    }

}
