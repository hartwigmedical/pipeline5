package com.hartwig.batch.api;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class RemoteLocationsDecorator implements RemoteLocations {

    private final RemoteLocations decorator;

    public RemoteLocationsDecorator(final RemoteLocations decorator) {
        this.decorator = decorator;
    }

    @Override
    public String getTumor() {
        return decorator.getTumor();
    }

    @Override
    public String getReference() {
        return decorator.getReference();
    }

    @Override
    public GoogleStorageLocation getGeneCopyNumberTsv() {
        return decorator.getGeneCopyNumberTsv();
    }

    @Override
    public GoogleStorageLocation getAmber() {
        return decorator.getAmber();
    }

    @Override
    public GoogleStorageLocation getCobalt() {
        return decorator.getCobalt();
    }

    @Override
    public GoogleStorageLocation getStructuralVariantsGridss() {
        return decorator.getStructuralVariantsGridss();
    }

    @Override
    public GoogleStorageLocation getStructuralVariantsGripss() {
        return decorator.getStructuralVariantsGripss();
    }

    @Override
    public GoogleStorageLocation getStructuralVariantsGripssRecovery() {
        return decorator.getStructuralVariantsGripssRecovery();
    }

    @Override
    public GoogleStorageLocation getStructuralVariantsGripssRecoveryIndex() {
        return decorator.getStructuralVariantsGripssRecoveryIndex();
    }

    @Override
    public GoogleStorageLocation getSomaticVariantsSage() {
        return decorator.getSomaticVariantsSage();
    }

    @Override
    public GoogleStorageLocation getSomaticVariantsPurple() {
        return decorator.getSomaticVariantsPurple();
    }

    @Override
    public GoogleStorageLocation getGermlineVariantsSage() {
        return decorator.getGermlineVariantsSage();
    }

    @Override
    public GoogleStorageLocation getReferenceAlignment() {
        return decorator.getReferenceAlignment();
    }

    @Override
    public GoogleStorageLocation getReferenceAlignmentIndex() {
        return decorator.getReferenceAlignmentIndex();
    }

    @Override
    public GoogleStorageLocation getTumorAlignment() {
        return decorator.getTumorAlignment();
    }

    @Override
    public GoogleStorageLocation getTumorAlignmentIndex() {
        return decorator.getTumorAlignmentIndex();
    }
}
