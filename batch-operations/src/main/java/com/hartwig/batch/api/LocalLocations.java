package com.hartwig.batch.api;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class LocalLocations {

    private final RemoteLocations locations;
    private final Set<String> commands = Sets.newLinkedHashSet();
    private boolean commandsGenerated = false;

    public LocalLocations(final RemoteLocations locations) {
        this.locations = locations;
    }

    public List<BashCommand> generateDownloadCommands() {
        commandsGenerated = true;
        return commands.stream().map(x -> (BashCommand) () -> x).collect(Collectors.toList());
    }

    public String getTumor() {
        return locations.getTumor();
    }

    public String getReference() {
        return locations.getReference();
    }

    public String getAmber() {
        return getDirectory(locations::getAmber, VmDirectories.INPUT + "/amber");
    }

    public String getCobalt() {
        return getDirectory(locations::getCobalt, VmDirectories.INPUT + "/cobalt");
    }

    public String getStructuralVariantsGridss() {
        return get(locations::getStructuralVariantsGridss);
    }

    public String getStructuralVariantsGripss() {
        return get(locations::getStructuralVariantsGripss);
    }

    public String getGeneCopyNumberTsv() {
        return get(locations::getGeneCopyNumberTsv);
    }

    public String getStructuralVariantsGripssRecovery() {
        String result = get(locations::getStructuralVariantsGripssRecovery);
        get(locations::getStructuralVariantsGripssRecoveryIndex);
        return result;
    }

    public String getReferenceAlignment() {
        String result = get(locations::getReferenceAlignment);
        get(locations::getReferenceAlignmentIndex);
        return result;
    }

    public String getTumorAlignment() {
        String result = get(locations::getTumorAlignment);
        get(locations::getTumorAlignmentIndex);
        return result;
    }

    public String getSomaticVariantsSage() {
        return get(locations::getSomaticVariantsSage);
    }

    public String getSomaticVariantsPurple() {
        String result =  get(locations::getSomaticVariantsPurple);
        get(() -> locations.getSomaticVariantsPurple().transform(x -> x.replace(".vcf.gz", ".vcf.gz.tbi")));
        return result;
    }

    public String getGermlineVariantsSage() {
        return get(locations::getGermlineVariantsSage);
    }

    private String get(final Supplier<GoogleStorageLocation> remotePath) {
        if (commandsGenerated) {
            throw new IllegalStateException("Cannot request new files after generating download commands");
        }

        GoogleStorageLocation location = remotePath.get();
        InputDownload result = new InputDownload(location);
        commands.add(result.asBash());
        return result.getLocalTargetPath();
    }

    private String getDirectory(final Supplier<GoogleStorageLocation> remotePath, final String localPath) {
        if (commandsGenerated) {
            throw new IllegalStateException("Cannot request new files after generating download commands");
        }

        GoogleStorageLocation location = remotePath.get();
        InputDownload result = new InputDownload(location, localPath);
        commands.add("mkdir -p " + localPath);
        commands.add(result.asBash());
        return result.getLocalTargetPath();
    }

}
