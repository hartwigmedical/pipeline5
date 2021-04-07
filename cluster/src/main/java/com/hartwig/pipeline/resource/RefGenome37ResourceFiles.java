package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.AMBER;
import static com.hartwig.pipeline.resource.ResourceNames.BACHELOR;
import static com.hartwig.pipeline.resource.ResourceNames.COBALT;
import static com.hartwig.pipeline.resource.ResourceNames.ENSEMBL_DATA_CACHE;
import static com.hartwig.pipeline.resource.ResourceNames.FUSIONS;
import static com.hartwig.pipeline.resource.ResourceNames.GC_PROFILES;
import static com.hartwig.pipeline.resource.ResourceNames.GENE_PANEL;
import static com.hartwig.pipeline.resource.ResourceNames.GENOTYPE_SNPS;
import static com.hartwig.pipeline.resource.ResourceNames.GIAB_HIGH_CONF;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_PON;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_REPEAT_MASKER_DB;
import static com.hartwig.pipeline.resource.ResourceNames.LINX;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;
import static com.hartwig.pipeline.resource.ResourceNames.SERVE;
import static com.hartwig.pipeline.resource.ResourceNames.SNPEFF;

public class RefGenome37ResourceFiles implements ResourceFiles {

    private static final String REF_GENOME_FASTA_37_FILE = "Homo_sapiens.GRCh37.GATK.illumina.fasta";

    @Override
    public RefGenomeVersion version() {
        return RefGenomeVersion.V37;
    }

    @Override
    public String versionDirectory() {
        return version().resources();
    }

    @Override
    public String refGenomeFile() {
        return formPath(REFERENCE_GENOME, REF_GENOME_FASTA_37_FILE);
    }

    @Override
    public String gcProfileFile() {
        return formPath(GC_PROFILES, "GC_profile.1000bp.37.cnp");
    }

    @Override
    public String diploidRegionsBed() {
        return formPath(COBALT, "DiploidRegions.37.bed.gz");
    }

    @Override
    public String amberHeterozygousLoci() {
        return formPath(AMBER, "GermlineHetPon.37.vcf.gz");
    }

    @Override
    public String amberSnpcheck() {
        return formPath(AMBER, "Amber.snpcheck.37.vcf");
    }

    @Override
    public String sageSomaticHotspots() {
        return formPath(SAGE, "KnownHotspots.somatic.37.vcf.gz");
    }

    @Override
    public String sageSomaticCodingPanel() {
        return formPath(SAGE, "ActionableCodingPanel.somatic.37.bed.gz");
    }

    @Override
    public String sageGermlineHotspots() {
        return formPath(SAGE, "KnownHotspots.germline.37.vcf.gz");
    }

    @Override
    public String sageGermlineCodingPanel() {
        return formPath(SAGE, "ActionableCodingPanel.germline.37.bed.gz");
    }

    @Override
    public String sageGermlineCoveragePanel() {
        return formPath(SAGE, "CoverageCodingPanel.germline.37.bed.gz");
    }

    @Override
    public String sageGermlineBlacklistVcf() {
        return formPath(SAGE, "KnownBlacklist.germline.37.vcf.gz");
    }

    @Override
    public String sageGermlineBlacklistBed() {
        return formPath(SAGE, "KnownBlacklist.germline.37.bed.gz");
    }

    @Override
    public String clinvarVcf() {
        return formPath(SAGE, "clinvar.37.vcf.gz");
    }

    @Override
    public String out150Mappability() {
        return formPath(MAPPABILITY, "out_150.mappability.37.bed.gz");
    }

    @Override
    public String sageGermlinePon() {
        return formPath(SAGE, "SageGermlinePon.1000x.37.vcf.gz");
    }

    @Override
    public String giabHighConfidenceBed() {
        return formPath(GIAB_HIGH_CONF, "NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz");
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
    public String gridssRepeatMaskerDb() {
        return formPath(GRIDSS_REPEAT_MASKER_DB, "37.fa.out");
    }

    @Override
    public String gridssBlacklistBed() {
        return formPath(GRIDSS_REPEAT_MASKER_DB, "ENCFF001TDO.37.bed");
    }

    @Override
    public String gridssBreakendPon() {
        return formPath(GRIDSS_PON, "gridss_pon_single_breakend.37.bed");
    }

    @Override
    public String gridssBreakpointPon() {
        return formPath(GRIDSS_PON, "gridss_pon_breakpoint.37.bedpe");
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
    public String fragileSites() {
        return formPath(LINX, "fragile_sites_hmf.37.csv");
    }

    @Override
    public String lineElements() {
        return formPath(LINX, "line_elements.37.csv");
    }

    @Override
    public String originsOfReplication() {
        return formPath(LINX, "heli_rep_origins.37.bed");
    }

    @Override
    public String ensemblDataCache() {
        return formPath(ENSEMBL_DATA_CACHE, "");
    }

    @Override
    public String knownFusionData() {
        return formPath(FUSIONS, "known_fusion_data.37.csv");
    }

    @Override
    public String knownFusionPairBedpe() {
        return formPath(FUSIONS, "known_fusions.37.bedpe");
    }

    @Override
    public String genotypeSnpsDB() {
        return formPath(GENOTYPE_SNPS, "26SNPtaq.vcf");
    }

    @Override
    public String driverGenePanel() {
        return formPath(GENE_PANEL, "DriverGenePanel.37.tsv");
    }

    @Override
    public String actionabilityDir() {
        return formPath(SERVE, "");
    }
}
