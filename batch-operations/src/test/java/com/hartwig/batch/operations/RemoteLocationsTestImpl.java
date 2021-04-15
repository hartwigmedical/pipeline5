package com.hartwig.batch.operations;

import com.hartwig.batch.api.RemoteLocations;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class RemoteLocationsTestImpl implements RemoteLocations {
    @Override
    public String getTumor() {
        return "COLO829v003T";
    }

    @Override
    public String getReference() {
        return "COLO829v003R";
    }

    @Override
    public GoogleStorageLocation getGeneCopyNumberTsv() {
        return GoogleStorageLocation.of("purple", "171006_COLO829/COLO929v003T.purple.cnv.gene.tsv");
    }

    @Override
    public GoogleStorageLocation getAmber() {
        return GoogleStorageLocation.of("amber", "171006_COLO829", true);
    }

    @Override
    public GoogleStorageLocation getCobalt() {
        return GoogleStorageLocation.of("cobalt", "171006_COLO829", true);
    }

    @Override
    public GoogleStorageLocation getStructuralVariantsGridss() {
        return GoogleStorageLocation.of("gridss", "171006_COLO829/COLO929v003T.gridss.somatic.vcf.gz");
    }

    @Override
    public GoogleStorageLocation getStructuralVariantsGripss() {
        return GoogleStorageLocation.of("gripss", "171006_COLO829/COLO929v003T.gripss.somatic.filtered.vcf.gz");
    }

    @Override
    public GoogleStorageLocation getStructuralVariantsGripssRecovery() {
        return GoogleStorageLocation.of("gripss", "171006_COLO829/COLO929v003T.gripss.somatic.vcf.gz");
    }

    @Override
    public GoogleStorageLocation getStructuralVariantsGripssRecoveryIndex() {
        return GoogleStorageLocation.of("gripss", "171006_COLO829/COLO929v003T.gripss.somatic.vcf.gz.tbi");
    }

    @Override
    public GoogleStorageLocation getSomaticVariantsSage() {
        return GoogleStorageLocation.of("sage", "171006_COLO829/COLO929v003T.sage.somatic.vcf.gz");
    }

    @Override
    public GoogleStorageLocation getSomaticVariantsPurple() {
        return GoogleStorageLocation.of("purple", "171006_COLO829/COLO929v003T.purple.somatic.vcf.gz");
    }

    @Override
    public GoogleStorageLocation getGermlineVariantsSage() {
        return GoogleStorageLocation.of("sage", "171006_COLO829/COLO929v003T.sage.germline.vcf.gz");
    }

    @Override
    public GoogleStorageLocation getReferenceAlignment() {
        return GoogleStorageLocation.of("alignment", "171006_COLO829/COLO929v003R.cram");
    }

    @Override
    public GoogleStorageLocation getReferenceAlignmentIndex() {
        return GoogleStorageLocation.of("alignment", "171006_COLO829/COLO929v003R.crai");
    }

    @Override
    public GoogleStorageLocation getTumorAlignment() {
        return GoogleStorageLocation.of("alignment", "171006_COLO829/COLO929v003T.cram");
    }

    @Override
    public GoogleStorageLocation getTumorAlignmentIndex() {
        return GoogleStorageLocation.of("alignment", "171006_COLO829/COLO929v003T.crai");
    }
}
