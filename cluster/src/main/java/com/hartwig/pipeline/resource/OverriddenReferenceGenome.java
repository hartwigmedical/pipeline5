package com.hartwig.pipeline.resource;

public class OverriddenReferenceGenome implements ResourceFiles {

    private final ResourceFiles decorated;
    private final String referenceGenomeFile;

    public OverriddenReferenceGenome(final ResourceFiles decorated, final String referenceGenomeFile) {
        this.decorated = decorated;
        this.referenceGenomeFile = referenceGenomeFile;
    }

    @Override
    public String refGenomeFile() {
        return referenceGenomeFile;
    }

    @Override
    public RefGenomeVersion version() {
        return decorated.version();
    }

    @Override
    public String versionDirectory() {
        return decorated.versionDirectory();
    }

    @Override
    public String gcProfileFile() {
        return decorated.gcProfileFile();
    }

    @Override
    public String amberHeterozygousLoci() {
        return decorated.amberHeterozygousLoci();
    }

    @Override
    public String gridssRepeatMaskerDb() {
        return decorated.gridssRepeatMaskerDb();
    }

    @Override
    public String gridssBlacklistBed() {
        return decorated.gridssBlacklistBed();
    }

    @Override
    public String gridssBreakendPon() {
        return decorated.gridssBreakendPon();
    }

    @Override
    public String gridssBreakpointPon() {
        return decorated.gridssBreakendPon();
    }

    @Override
    public String snpEffDb() {
        return decorated.snpEffDb();
    }

    @Override
    public String snpEffVersion() {
        return decorated.snpEffVersion();
    }

    @Override
    public String snpEffConfig() {
        return decorated.snpEffConfig();
    }

    @Override
    public String sageKnownHotspots() {
        return decorated.sageKnownHotspots();
    }

    @Override
    public String sageActionableCodingPanel() {
        return decorated.sageActionableCodingPanel();
    }

    @Override
    public String out150Mappability() {
        return decorated.out150Mappability();
    }

    @Override
    public String sageGermlinePon() {
        return decorated.sageGermlinePon();
    }

    @Override
    public String giabHighConfidenceBed() {
        return decorated.giabHighConfidenceBed();
    }

    @Override
    public String knownFusionPairBedpe() {
        return decorated.knownFusionPairBedpe();
    }

    @Override
    public String bachelorConfig() {
        return decorated.bachelorConfig();
    }

    @Override
    public String bachelorClinvarFilters() {
        return decorated.bachelorClinvarFilters();
    }

    @Override
    public String ensemblDataCache() {
        return decorated.ensemblDataCache();
    }

    @Override
    public String fragileSites() {
        return decorated.fragileSites();
    }

    @Override
    public String lineElements() {
        return decorated.lineElements();
    }

    @Override
    public String originsOfReplication() {
        return decorated.originsOfReplication();
    }

    @Override
    public String knownFusionData() {
        return decorated.knownFusionData();
    }

    @Override
    public String genotypeSnpsDB() {
        return decorated.genotypeSnpsDB();
    }

    @Override
    public String driverGenePanel() {
        return decorated.driverGenePanel();
    }
}
