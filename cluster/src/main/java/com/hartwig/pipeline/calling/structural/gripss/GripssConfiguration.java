package com.hartwig.pipeline.calling.structural.gripss;

import java.util.function.Function;

import com.hartwig.pipeline.calling.sage.OutputTemplate;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.resource.ResourceFiles;

import org.immutables.value.Value;

@Value.Immutable
public interface GripssConfiguration {

    String GRIPSS_SOMATIC_FILTERED = ".gripss.filtered.somatic.";
    String GRIPSS_SOMATIC_UNFILTERED = ".gripss.somatic.";
    String GRIPSS_GERMLINE_FILTERED = ".gripss.filtered.germline.";
    String GRIPSS_GERMLINE_UNFILTERED = ".gripss.germline.";
    String GERMLINE_NAMESPACE = "gripss_germline";
    String SOMATIC_NAMESPACE = "gripss_somatic";

    String namespace();

    OutputTemplate filteredVcf();

    OutputTemplate unfilteredVcf();

    DataType filteredDatatype();

    DataType unfilteredDatatype();

    Function<SomaticRunMetadata, GripssCommandBuilder> commandBuilder();

    static GripssConfiguration somatic(final ResourceFiles resourceFiles) {
        return ImmutableGripssConfiguration.builder()
                .filteredVcf(m -> m.tumor().sampleName() + GRIPSS_SOMATIC_FILTERED + FileTypes.GZIPPED_VCF)
                .unfilteredVcf(m -> m.tumor().sampleName() + GRIPSS_SOMATIC_UNFILTERED + FileTypes.GZIPPED_VCF)
                .filteredDatatype(DataType.SOMATIC_STRUCTURAL_VARIANTS_GRIPSS)
                .unfilteredDatatype(DataType.SOMATIC_STRUCTURAL_VARIANTS_GRIPSS_RECOVERY)
                .namespace(SOMATIC_NAMESPACE)
                .commandBuilder(m -> GripssCommandBuilder.newBuilder(resourceFiles, "somatic")
                        .sample(m.tumor().sampleName())
                        .maybeReference(m.maybeReference().map(SingleSampleRunMetadata::sampleName)))
                .build();
    }

    static GripssConfiguration germline(final ResourceFiles resourceFiles) {
        return ImmutableGripssConfiguration.builder()
                .filteredVcf(m -> m.reference().sampleName() + GRIPSS_GERMLINE_FILTERED + FileTypes.GZIPPED_VCF)
                .unfilteredVcf(m -> m.reference().sampleName() + GRIPSS_GERMLINE_UNFILTERED + FileTypes.GZIPPED_VCF)
                .filteredDatatype(DataType.GERMLINE_STRUCTURAL_VARIANTS_GRIPSS)
                .unfilteredDatatype(DataType.GERMLINE_STRUCTURAL_VARIANTS_GRIPSS_RECOVERY)
                .namespace(GERMLINE_NAMESPACE)
                .commandBuilder(m -> GripssCommandBuilder.newBuilder(resourceFiles, "germline").sample(m.reference().sampleName()))
                .build();
    }
}