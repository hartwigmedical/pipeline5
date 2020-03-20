package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.COSMIC;
import static com.hartwig.pipeline.resource.ResourceNames.DBNSFP;
import static com.hartwig.pipeline.resource.ResourceNames.SNPEFF;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public interface ResourceFiles {

    static String of(String name, String file) {
        return String.format("%s/%s/%s", VmDirectories.RESOURCES, name, file);
    }

    static String of(String name) {
        return of(name, "");
    }

    RefGenomeVersion version();

    String versionDirectory();

    String refGenomeFile();
    String gcProfileFile();
    String amberHeterozygousLoci();
    String gridssRepeatMaskerDb();
    String gridssBlacklistBed();
    String snpEffDb();
    String snpEffVersion();
    String sageKnownHotspots();
    String sageActionableCodingPanel();
    String out150Mappability();
    String sageGermlinePon();
    String giabHighConfidenceBed();

    String SNPEFF_CONFIG = ResourceFiles.of(SNPEFF, "snpEff.config");
    String DBNSFP_VCF = ResourceFiles.of(DBNSFP, "dbNSFP2.9.txt.gz");
    String COSMIC_VCF_GZ = ResourceFiles.of(COSMIC, "CosmicCodingMuts_v85_collapsed.vcf.gz");
}
