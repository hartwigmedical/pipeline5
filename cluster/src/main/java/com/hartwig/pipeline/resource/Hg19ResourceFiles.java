package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.AMBER;
import static com.hartwig.pipeline.resource.ResourceNames.BACHELOR;
import static com.hartwig.pipeline.resource.ResourceNames.COBALT;
import static com.hartwig.pipeline.resource.ResourceNames.ENSEMBL;
import static com.hartwig.pipeline.resource.ResourceNames.GC_PROFILE;
import static com.hartwig.pipeline.resource.ResourceNames.GENE_PANEL;
import static com.hartwig.pipeline.resource.ResourceNames.GENOTYPE_SNPS;
import static com.hartwig.pipeline.resource.ResourceNames.GIAB_HIGH_CONF;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_PON;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_REPEAT_MASKER_DB;
import static com.hartwig.pipeline.resource.ResourceNames.KNOWLEDGEBASES;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;
import static com.hartwig.pipeline.resource.ResourceNames.SNPEFF;
import static com.hartwig.pipeline.resource.ResourceNames.SV;

public class Hg19ResourceFiles implements ResourceFiles {
    public static final String HG19_DIRECTORY = "hg19";
    private static final String REF_GENOME_FASTA_HG19_FILE = "Homo_sapiens.GRCh37.GATK.illumina.fasta";

    @Override
    public RefGenomeVersion version() {
        return RefGenomeVersion.HG19;
    }

    @Override
    public String versionDirectory() {
        return HG19_DIRECTORY;
    }

    @Override
    public String refGenomeFile() {
        return formPath(REFERENCE_GENOME, REF_GENOME_FASTA_HG19_FILE);
    }

    @Override
    public String gcProfileFile() {
        return formPath(GC_PROFILE, "GC_profile.1000bp.cnp");
    }

    @Override
    public String diploidRegionsBed() {
        return formPath(COBALT, "DiploidRegions.hg19.bed.gz");
    }

    @Override
    public String amberHeterozygousLoci() {
        return formPath(AMBER, "GermlineHetPon.hg19.vcf.gz");
    }

    @Override
    public String gridssRepeatMaskerDb() {
        return formPath(GRIDSS_REPEAT_MASKER_DB, "hg19.fa.out");
    }

    @Override
    public String gridssBlacklistBed() {
        return formPath(GRIDSS_REPEAT_MASKER_DB, "ENCFF001TDO.bed");
    }

    @Override
    public String gridssBreakendPon() {
        return formPath(GRIDSS_PON, "gridss_pon_single_breakend.hg19.bed");
    }

    @Override
    public String gridssBreakpointPon() {
        return formPath(GRIDSS_PON, "gridss_pon_breakpoint.hg19.bedpe");
    }

    @Override
    public String snpEffDb() {
        return formPath(SNPEFF, "snpEff_v4_3_GRCh37.75.zip");
    }

    @Override
    public String snpEffVersion() {
        return "GRCh37.75";
    }

    @Override
    public String snpEffConfig() {
        return formPath(SNPEFF, "snpEff.config");
    }

    @Override
    public String sageSomaticHotspots() {
        return formPath(SAGE, "KnownHotspots.somatic.hg19.vcf.gz");
    }

    @Override
    public String sageSomaticCodingPanel() {
        return formPath(SAGE, "ActionableCodingPanel.somatic.hg19.bed.gz");
    }

    @Override
    public String sageGermlineHotspots() {
        return formPath(SAGE, "KnownHotspots.germline.hg19.vcf.gz");
    }

    @Override
    public String sageGermlineCodingPanel() {
        return formPath(SAGE, "ActionableCodingPanel.germline.hg19.bed.gz");
    }

    @Override
    public String sageGermlineBlacklistVcf() {
        return formPath(SAGE, "KnownBlacklist.germline.hg19.vcf.gz");
    }

    @Override
    public String sageGermlineBlacklistBed() {
        return formPath(SAGE, "KnownBlacklist.germline.hg19.bed.gz");
    }

    @Override
    public String clinvarVcf() {
        return formPath(SAGE, "clinvar.hg19.vcf.gz");
    }

    @Override
    public String out150Mappability() {
        return formPath(MAPPABILITY, "out_150_hg19.mappability.bed.gz");
    }

    @Override
    public String sageGermlinePon() {
        return formPath(SAGE, "SageGermlinePon.hg19.1000x.vcf.gz");
    }

    @Override
    public String giabHighConfidenceBed() {
        return formPath(GIAB_HIGH_CONF, "NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz");
    }

    @Override
    public String knownFusionPairBedpe() {
        return formPath(KNOWLEDGEBASES, "KnownFusionPairs.hg19.bedpe");
    }

    @Override
    public String bachelorConfig() {
        return formPath(BACHELOR, "bachelor_hmf.xml");
    }

    @Override
    public String bachelorClinvarFilters() {
        return formPath(BACHELOR, "bachelor_clinvar_filters.csv");
    }

    @Override
    public String ensemblDataCache() {
        return formPath(ENSEMBL, "ensembl_data_cache");
    }

    @Override
    public String fragileSites() {
        return formPath(SV, "fragile_sites_hmf.csv");
    }

    @Override
    public String lineElements() {
        return formPath(SV, "line_elements.csv");
    }

    @Override
    public String originsOfReplication() {
        return formPath(SV, "heli_rep_origins.bed");
    }

    @Override
    public String genotypeSnpsDB() {
        return formPath(GENOTYPE_SNPS, "26SNPtaq.vcf");
    }

    @Override
    public String driverGenePanel() {
        return formPath(GENE_PANEL, "DriverGenePanel.hg19.tsv");
    }

    @Override
    public String knownFusionData() {
        return formPath(KNOWLEDGEBASES, "known_fusion_data.csv");
    }
}
