package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.AMBER_PON;
import static com.hartwig.pipeline.resource.ResourceNames.BACHELOR;
import static com.hartwig.pipeline.resource.ResourceNames.BEDS;
import static com.hartwig.pipeline.resource.ResourceNames.ENSEMBL;
import static com.hartwig.pipeline.resource.ResourceNames.GC_PROFILE;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_PON;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_REPEAT_MASKER_DB;
import static com.hartwig.pipeline.resource.ResourceNames.KNOWLEDGEBASES;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;
import static com.hartwig.pipeline.resource.ResourceNames.SNPEFF;
import static com.hartwig.pipeline.resource.ResourceNames.SV;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class Hg37ResourceFiles implements ResourceFiles {
    public static final String HG37_DIRECTORY = "hg37";

    public RefGenomeVersion version() { return RefGenomeVersion.HG37; }

    public String versionDirectory() { return HG37_DIRECTORY; }

    private String formPath(String name, String file) {
        return String.format("%s/%s/%s/%s", VmDirectories.RESOURCES, name, versionDirectory(), file);
    }

    private static final String REF_GENOME_FASTA_HG37_FILE = "Homo_sapiens.GRCh37.GATK.illumina.fasta";

    public String refGenomeFile() { return formPath(REFERENCE_GENOME, REF_GENOME_FASTA_HG37_FILE); }

    public String gcProfileFile() { return formPath(GC_PROFILE,"GC_profile.1000bp.cnp"); }

    public String amberHeterozygousLoci() { return formPath(AMBER_PON, "GermlineHetPon.hg19.vcf.gz"); }

    public String gridssRepeatMaskerDb() {
        return formPath(GRIDSS_REPEAT_MASKER_DB, "hg19.fa.out");
    }

    public String gridssBlacklistBed() {
        return formPath(GRIDSS_REPEAT_MASKER_DB, "ENCFF001TDO.hg37.bed");
    }

    public String gridssBreakendPon() {
        return formPath(GRIDSS_PON, "gridss_pon_single_breakend.hg19.bed");
    }

    public String gridssBreakpointPon() {
        return formPath(GRIDSS_PON, "gridss_pon_breakpoint.hg19.bedpe");
    }

    public String snpEffDb() {
        return formPath(SNPEFF, "snpEff_v4_3_GRCh37.75.zip");
    }

    public String snpEffVersion() {
        return "GRCh37.75";
    }

    public String snpEffConfig() {
        return formPath(SNPEFF, "snpEff.config");
    }

    public String sageKnownHotspots() {
        return formPath(SAGE, "KnownHotspots.hg19.vcf.gz");
    }

    public String sageActionableCodingPanel() {
        return formPath(SAGE, "ActionableCodingPanel.hg19.bed.gz");
    }

    public String out150Mappability() {
        return formPath(MAPPABILITY, "out_150_hg19.mappability.bed.gz");
    }

    public String sageGermlinePon() {
        return formPath(SAGE, "SageGermlinePon.hg19.1000x.vcf.gz");
    }

    public String giabHighConfidenceBed() {
        return formPath(BEDS, "NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz");
    }

    public String knownFusionPairBedpe() { return formPath(KNOWLEDGEBASES,"KnownFusionPairs.hg19.bedpe"); }

    public String bachelorConfig() { return formPath(BACHELOR, "bachelor_hmf.xml"); }
    public String bachelorClinvarFilters() { return formPath(BACHELOR, "bachelor_clinvar_filters.csv"); }

    public String ensemblDataCache() { return formPath(ENSEMBL, "ensembl_data_cache"); }

    public String fragileSites() { return formPath(SV, "fragile_sites_hmf.csv"); }
    public String lineElements() { return formPath(SV, "line_elements.csv"); }
    public String originsOfReplication() { return formPath(SV, "heli_rep_origins.bed"); }

}
