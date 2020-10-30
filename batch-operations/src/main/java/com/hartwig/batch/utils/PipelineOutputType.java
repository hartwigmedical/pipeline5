package com.hartwig.batch.utils;

public enum PipelineOutputType
{
    SOMATIC_VARIANTS_SAGE,
    SOMATIC_VARIANTS_PURPLE,
    SV_VARIANTS_PURPLE,
    ALIGNED_READS,
    GERMLINE_VCF;

    public static final String SOMATIC_VARIANTS_PURPLE_STR = "somatic_variants_purple";
    public static final String SAGE_VCF_STR = "somatic_variants_sage";
    public static final String SV_PURPLE_STR = "structural_variants_purple";
    public static final String GERMINE_VCF_STR = "germline_variants";
    public static final String SV_GRIDSS_STR = "structural_variants_gridss";
    public static final String ALIGNED_READS_STR = "aligned_reads";

    public static String getApiFileKey(final PipelineOutputType type)
    {
        switch(type)
        {
            case GERMLINE_VCF: return GERMINE_VCF_STR;
            case SV_VARIANTS_PURPLE: return SV_PURPLE_STR;
            case ALIGNED_READS: return ALIGNED_READS_STR;
            case SOMATIC_VARIANTS_SAGE: return SAGE_VCF_STR;
            case SOMATIC_VARIANTS_PURPLE: return SOMATIC_VARIANTS_PURPLE_STR;
            
            default: return "";
        }
    }


    // somatic_variants_sage eg COLO829T.sage.somatic.filtered.vcf.gz
    // aligned_reads": {"COLO829R": {"path": "gs://hmf-output-2020-33/COLO829R/cram/COLO829R.cram
    // gs://hmf-output-2020-33/COLO829T/cram/COLO829T.cram", "version": "5.12"}}, 
    // structural_variants_gridss - eg gs://wide01010852t-dna-analysis/5.15/gridss/COLO829T.gridss.unfiltered.vcf.gz
    // B_ALLELE_FREQUENCY eg gs://wide01010852t-dna-analysis/5.15/amber
    // structural_variants_purple eg gs://wide01010852t-dna-analysis/5.15/purple/COLO829T.purple.sv.vcf.gz
    // structural_variants_gripss eg gs://wide01010852t-dna-analysis/5.15/gripss/COLO829T.gripss.somatic.filtered.vcf.gz
    // wgsmetrics eg gs://hmf-output-2020-33/COLO829R/bam_metrics/COLO829R.wgsmetrics
    // structural_variants_gripss_recovery eg gs://wide01010852t-dna-analysis/5.15/gripss/COLO829T.gripss.somatic.vcf.gz
    // READ_DEPTH_RATIO eg gs://wide01010852t-dna-analysis/5.15/cobalt
    // germline_variants eg gs://hmf-output-2020-33/COLO829R/germline_caller/COLO829R.germline.vcf.gz
    
}
