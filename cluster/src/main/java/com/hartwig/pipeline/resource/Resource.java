package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.COSMIC;
import static com.hartwig.pipeline.resource.ResourceNames.DBNSFP;
import static com.hartwig.pipeline.resource.ResourceNames.DBSNPS;
import static com.hartwig.pipeline.resource.ResourceNames.GC_PROFILE;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SNPEFF;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public interface Resource {

    static String of(String name, String file) {
        return String.format("%s/%s/%s", VmDirectories.RESOURCES, name, file);
    }

    static String of(String name) {
        return of(name, "");
    }

    String REFERENCE_GENOME_FASTA = Resource.of(REFERENCE_GENOME, "Homo_sapiens.GRCh37.GATK.illumina.fasta");
    String GC_PROFILE_CNP = Resource.of(GC_PROFILE, "GC_profile.1000bp.cnp");
    String SNPEFF_CONFIG = Resource.of(SNPEFF, "snpEff.config");
    String SNPEFF_DB = Resource.of(SNPEFF, "snpEff_v4_3_GRCh37.75.zip");
    String DBSNPS_VCF = Resource.of(DBSNPS, "dbsnp_137.b37.vcf");
    String DBNSFP_VCF = Resource.of(DBNSFP, "dbNSFP2.9.txt.gz");
    String COSMIC_VCF_GZ = Resource.of(COSMIC, "CosmicCodingMuts_v85_collapsed.vcf.gz");
}
