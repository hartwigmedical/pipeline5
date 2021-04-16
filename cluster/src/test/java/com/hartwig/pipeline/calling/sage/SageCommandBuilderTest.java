package com.hartwig.pipeline.calling.sage;

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
    private static final String REFERENCE_SAGE_COMMAND = "java -Xmx15G -cp /opt/tools/sage/2.8/sage.jar com.hartwig.hmftools.sage.SageApplication -tumor COLO829v003R -tumor_bam /data/input/COLO829v003R.bam -reference COLO829v003T -reference_bam /data/input/COLO829v003T.bam -hotspots /opt/resources/sage/37/KnownHotspots.germline.37.vcf.gz -panel_bed /opt/resources/sage/37/ActionableCodingPanel.germline.37.bed.gz -hotspot_min_tumor_qual 50 -panel_min_tumor_qual 75 -hotspot_max_germline_vaf 100 -hotspot_max_germline_rel_raw_base_qual 100 -panel_max_germline_vaf 100 -panel_max_germline_rel_raw_base_qual 100 -mnv_filter_enabled false -high_confidence_bed /opt/resources/giab_high_conf/37/NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -out /data/output/COLO829v003R.out.vcf.gz -assembly hg19 -threads $(grep -c '^processor' /proc/cpuinfo) -panel_only";


    @Test
    public void testGermlineBam() {
        SageCommandBuilder victim = new SageCommandBuilder(TestInputs.REF_GENOME_37_RESOURCE_FILES);
        victim.germlineMode(REFERENCE, REFERENCE_BAM, TUMOR, TUMOR_BAM);
        List<String> bash = victim.build(REFERENCE_OUT).stream().map(BashCommand::asBash).collect(Collectors.toList());
        assertEquals(1, bash.size());
        assertEquals(bash.get(0), REFERENCE_SAGE_COMMAND);
    }

    @Test
    public void testGermlineCram() {
        SageCommandBuilder victim = new SageCommandBuilder(TestInputs.REF_GENOME_37_RESOURCE_FILES);
        victim.germlineMode(REFERENCE, REFERENCE_BAM.replace(".bam", ".cram"), TUMOR, TUMOR_BAM.replace(".bam", ".cram"));
        List<String> bash = victim.build(REFERENCE_OUT).stream().map(BashCommand::asBash).collect(Collectors.toList());
        assertEquals(5, bash.size());

        assertEquals(bash.get(0), "/opt/tools/samtools/1.10/samtools view -T /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -L /opt/resources/sage/37/SlicePanel.germline.37.bed.gz -o /data/input/COLO829v003T.bam -u -@ $(grep -c '^processor' /proc/cpuinfo) -M /data/input/COLO829v003T.cram");
        assertEquals(bash.get(1), "/opt/tools/samtools/1.10/samtools index /data/input/COLO829v003T.bam");
        assertEquals(bash.get(2), "/opt/tools/samtools/1.10/samtools view -T /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -L /opt/resources/sage/37/SlicePanel.germline.37.bed.gz -o /data/input/COLO829v003R.bam -u -@ $(grep -c '^processor' /proc/cpuinfo) -M /data/input/COLO829v003R.cram");
        assertEquals(bash.get(3), "/opt/tools/samtools/1.10/samtools index /data/input/COLO829v003R.bam");
        assertEquals(bash.get(4), REFERENCE_SAGE_COMMAND);
    }

}
