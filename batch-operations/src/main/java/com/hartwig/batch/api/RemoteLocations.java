package com.hartwig.batch.api;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

public interface RemoteLocations {

    String getTumor();

    String getReference();

    GoogleStorageLocation getAmber();

    GoogleStorageLocation getCobalt();

    GoogleStorageLocation getGeneCopyNumberTsv();

    GoogleStorageLocation getStructuralVariantsGridss();

    GoogleStorageLocation getStructuralVariantsGripss();

    GoogleStorageLocation getStructuralVariantsGripssRecovery();

    GoogleStorageLocation getStructuralVariantsGripssRecoveryIndex();

    GoogleStorageLocation getSomaticVariantsSage();

    GoogleStorageLocation getSomaticVariantsPurple();

    GoogleStorageLocation getGermlineVariantsSage();

    GoogleStorageLocation getReferenceAlignment();

    GoogleStorageLocation getReferenceAlignmentIndex();

    GoogleStorageLocation getTumorAlignment();

    GoogleStorageLocation getTumorAlignmentIndex();

}
