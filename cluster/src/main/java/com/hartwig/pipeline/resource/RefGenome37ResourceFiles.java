package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.AMBER;
import static com.hartwig.pipeline.resource.ResourceNames.COBALT;
import static com.hartwig.pipeline.resource.ResourceNames.ENSEMBL_DATA_CACHE;
import static com.hartwig.pipeline.resource.ResourceNames.FUSIONS;
import static com.hartwig.pipeline.resource.ResourceNames.GC_PROFILES;
import static com.hartwig.pipeline.resource.ResourceNames.GENE_PANEL;
import static com.hartwig.pipeline.resource.ResourceNames.GENOTYPE_SNPS;
import static com.hartwig.pipeline.resource.ResourceNames.GIAB_HIGH_CONF;
import static com.hartwig.pipeline.resource.ResourceNames.GNOMAD;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_PON;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_REPEAT_MASKER_DB;
import static com.hartwig.pipeline.resource.ResourceNames.LILAC;
import static com.hartwig.pipeline.resource.ResourceNames.LINX;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.PURPLE;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;
import static com.hartwig.pipeline.resource.ResourceNames.SERVE;

public class RefGenome37ResourceFiles implements ResourceFiles {

    private static final String REF_GENOME_FASTA_37_FILE = "Homo_sapiens.GRCh37.GATK.illumina.fasta";

    @Override
    public RefGenomeVersion version() {
        return RefGenomeVersion.V37;
    }

    @Override
    public String versionDirectory() {
        return version().numeric();
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
    public String sageSomaticHotspots() {
        return formPath(SAGE, "KnownHotspots.somatic.37.vcf.gz");
    }

    @Override
    public String sageGermlineHotspots() {
        return formPath(SAGE, "KnownHotspots.germline.37.vcf.gz");
    }

    @Override
    public String sagePanelBed() {
        return formPath(SAGE, "ActionableCodingPanel.37.bed.gz");
    }

    @Override
    public String sageGeneCoverageBed() {
        return formPath(SAGE, "CoverageCodingPanel.37.bed.gz");
    }

    @Override
    public String germlineBlacklistVcf() {
        return formPath(SAGE, "KnownBlacklist.germline.37.vcf.gz");
    }

    @Override
    public String germlineBlacklistBed() {
        return formPath(SAGE, "KnownBlacklist.germline.37.bed");
    }

    @Override
    public String clinvarVcf() {
        return formPath(SAGE, "clinvar.37.vcf.gz");
    }

    @Override
    public String mappabilityBed() {
        return formPath(MAPPABILITY, "mappability_150.37.bed.gz");
    }

    @Override
    public String sageGermlinePon() {
        return formPath(SAGE, "SageGermlinePon.1000x.37.vcf.gz");
    }

    @Override
    public String germlinePon() {
        return formPath(SAGE, "SageGermlinePon.1000x.37.tsv.gz");
    }

    @Override
    public String somaticPonArtefacts() {
        return formPath(SAGE, "PanelArtefacts.37.tsv");
    }

    @Override
    public String gnomadPonCache() {
        return formPath(GNOMAD, "");
    }

    @Override
    public String giabHighConfidenceBed() {
        return formPath(GIAB_HIGH_CONF, "NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz");
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
    public String svBreakendPon() {
        return formPath(GRIDSS_PON, "gridss_pon_single_breakend.37.bed.gz");
    }

    @Override
    public String svBreakpointPon() {
        return formPath(GRIDSS_PON, "gridss_pon_breakpoint.37.bedpe.gz");
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

    @Override
    public String hlaRegionBed() {
        return formPath(LILAC, "hla.37.bed");
    }

    @Override
    public String purpleCohortGermlineDeletions() {
        return formPath(PURPLE, "cohort_germline_del_freq.37.csv");
    }

    @Override
    public String targetRegionsBed() {
        throw targetRegionsNotSupported();
    }

    @Override
    public String targetRegionsNormalisation() {
        throw targetRegionsNotSupported();
    }

    @Override
    public String targetRegionsRatios() {
        throw targetRegionsNotSupported();
    }

    @Override
    public String targetRegionsMsiIndels() {
        throw targetRegionsNotSupported();
    }

    private static UnsupportedOperationException targetRegionsNotSupported() {
        return new UnsupportedOperationException("Target regions are not supported in HG37");
    }
}
