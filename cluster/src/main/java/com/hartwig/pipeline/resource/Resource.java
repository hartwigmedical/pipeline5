package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.AMBER_PON;
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

    RefGenomeVersion version();

    String refGenomeFile();
    String gcProfileFile();
    String germlineHetPon();
    String gridssRepeatMaskerDb();
    String snpEffDb();
    String sageKnownHotspots();
    String sageActionableCodingPanel();
    String out150Mappability();
    String sageGermlinePon();

    String REFERENCE_GENOME_FASTA = Resource.of(REFERENCE_GENOME, "Homo_sapiens.GRCh37.GATK.illumina.fasta");
    String SNPEFF_CONFIG = Resource.of(SNPEFF, "snpEff.config");
    String DBSNPS_VCF = Resource.of(DBSNPS, "dbsnp_137.b37.vcf");
    String DBNSFP_VCF = Resource.of(DBNSFP, "dbNSFP2.9.txt.gz");
    String COSMIC_VCF_GZ = Resource.of(COSMIC, "CosmicCodingMuts_v85_collapsed.vcf.gz");
}
