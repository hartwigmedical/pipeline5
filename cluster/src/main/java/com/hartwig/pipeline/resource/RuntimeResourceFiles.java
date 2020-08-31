package com.hartwig.pipeline.resource;

import static java.lang.String.format;

import java.util.Collections;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.storage.StorageProvider;

public class RuntimeResourceFiles implements ResourceFiles {
    private static final String RESOURCE_DESCRIPTOR = "resources.json";
    private Map<String, String> parsedDescriptor;
    private Arguments arguments;

    public RuntimeResourceFiles(Arguments arguments) {
        this.arguments = arguments;
        parsedDescriptor = Collections.emptyMap();
    }

    @Override
    public RefGenomeVersion version() {
        return RefGenomeVersion.valueOf(fetch("refGenomeVersion").toUpperCase());
    }

    @Override
    public String versionDirectory() {
        return path("refGenomeDirectory");
    }

    @Override
    public String refGenomeFile() {
        return path("refGenomeFile");
    }

    @Override
    public String gcProfileFile() {
        return path("gcProfileFile");
    }

    @Override
    public String amberHeterozygousLoci() {
        return path("amberHeterozygousLoci");
    }

    @Override
    public String gridssRepeatMaskerDb() {
        return path("gridssRepeatMaskerDb");
    }

    @Override
    public String gridssBlacklistBed() {
        return path("gridssBlacklistBed");
    }

    @Override
    public String gridssBreakendPon() {
        return path("gridssBreakendPon");
    }

    @Override
    public String gridssBreakpointPon() {
        return path("gridssBreakpointPon");
    }

    @Override
    public String snpEffDb() {
        return path("snpEffDb");
    }

    @Override
    public String snpEffVersion() {
        return fetch("snpEffVersion");
    }

    @Override
    public String snpEffConfig() {
        return path("snpEffConfig");
    }

    @Override
    public String sageKnownHotspots() {
        return path("sageKnownHotspots");
    }

    @Override
    public String sageActionableCodingPanel() {
        return path("sageActionableCodingPanel");
    }

    @Override
    public String out150Mappability() {
        return path("out150Mappability");
    }

    @Override
    public String sageGermlinePon() {
        return path("sageGermlinePon");
    }

    @Override
    public String giabHighConfidenceBed() {
        return path("giabHighConfidenceBed");
    }

    @Override
    public String knownFusionPairBedpe() {
        return path("knownFusionPairBedpe");
    }

    @Override
    public String bachelorConfig() {
        return path("bachelorConfig");
    }

    @Override
    public String bachelorClinvarFilters() {
        return path("bachelorClinvarFilters");
    }

    @Override
    public String ensemblDataCache() {
        return path("ensemblDataCache");
    }

    @Override
    public String fragileSites() {
        return path("fragileSites");
    }

    @Override
    public String lineElements() {
        return path("lineElements");
    }

    @Override
    public String originsOfReplication() {
        return path("originsOfReplication");
    }

    @Override
    public String knownFusionData() {
        return path("knownFusionData");
    }

    @Override
    public String genotypeSnpsDB() {
        return path("genotypeSnpsDB");
    }

    private String path(String key) {
        return format("%s/%s", VmDirectories.RESOURCES, fetch(key));
    }

    private String fetch(String key) {
        if (parsedDescriptor.isEmpty()) {
            try {
                Storage storage = StorageProvider.from(arguments, CredentialProvider.from(arguments).get()).get();
                parsedDescriptor = ObjectMappers.get()
                        .readValue(storage.get(arguments.customResourcesBucket().get()).get(RESOURCE_DESCRIPTOR).getContent(),
                                new TypeReference<Map<String, String>>() {
                                });
            } catch (Exception e) {
                throw new RuntimeException("Failed to fetch custom resources", e);
            }
        }
        if (!parsedDescriptor.containsKey(key)) {
            throw new IllegalStateException(format("Required key [%s] not found in resources", key));
        }
        return parsedDescriptor.get(key);
    }
}
