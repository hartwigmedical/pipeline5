package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_CONFIG;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.SV;
import static com.hartwig.pipeline.resource.ResourceNames.VIRUS_REFERENCE_GENOME;

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

    String diploidRegionsBed();

    String amberHeterozygousLoci();

    String amberSnpcheck();

    String gridssRepeatMaskerDb();

    default String gridssRepeatMaskerDbBed() {
        return gridssRepeatMaskerDb() + ".bed";
    }

    default String gridssVirusRefGenomeFile() {
        return of(VIRUS_REFERENCE_GENOME, "human_virus.fa");
    }

    default String gridssPropertiesFile() {
        return of(GRIDSS_CONFIG, "gridss.properties");
    }

    String gridssBlacklistBed();

    String gridssBreakendPon();

    String gridssBreakpointPon();

    String snpEffDb();

    String snpEffVersion();

    String snpEffConfig();

    String sageSomaticHotspots();

    String sageSomaticCodingPanel();

    String sageGermlineHotspots();

    String sageGermlineCodingPanel();

    String sageGermlineCoveragePanel();

    String sageGermlineBlacklistVcf();

    String sageGermlineBlacklistBed();

    String clinvarVcf();

    String out150Mappability();

    default String mappabilityHDR() {
        return of(MAPPABILITY, "mappability.hdr");
    }

    String sageGermlinePon();

    String giabHighConfidenceBed();

    String knownFusionPairBedpe();

    String bachelorConfig();

    String bachelorClinvarFilters();

    String ensemblDataCache();

    String fragileSites();

    String lineElements();

    String originsOfReplication();

    String knownFusionData();

    String genotypeSnpsDB();

    String driverGenePanel();

    default String viralHostRefs() {
        return of(SV, "viral_host_ref.csv");
    }

    default String formPath(String name, String file) {
        return String.format("%s/%s/%s/%s", VmDirectories.RESOURCES, name, versionDirectory(), file);
    }
}
