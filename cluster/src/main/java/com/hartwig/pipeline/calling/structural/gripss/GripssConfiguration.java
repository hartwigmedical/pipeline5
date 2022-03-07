package com.hartwig.pipeline.calling.structural.gripss;

import java.util.function.BiFunction;

import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.sage.OutputTemplate;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;

import org.immutables.value.Value;

@Value.Immutable
public interface GripssConfiguration {

    String GRIPSS_SOMATIC_FILTERED = ".gripss.filtered.somatic.";
    String GRIPSS_SOMATIC_UNFILTERED = ".gripss.somatic.";
    String GRIPSS_GERMLINE_FILTERED = ".gripss.filtered.germline.";
    String GRIPSS_GERMLINE_UNFILTERED = ".gripss.germline.";
    String GERMLINE_NAMESPACE = "gripss_germline";

    String namespace();

    BiFunction<BashStartupScript, ResultsDirectory, VirtualMachineJobDefinition> jobDefinition();

    OutputTemplate filteredVcf();

    OutputTemplate unfilteredVcf();

    static GripssConfiguration somatic() {
        return ImmutableGripssConfiguration.builder()
                .filteredVcf(m -> m.tumor().sampleName() + GRIPSS_SOMATIC_UNFILTERED + FileTypes.GZIPPED_VCF)
                .unfilteredVcf(m -> m.tumor().sampleName() + GRIPSS_SOMATIC_FILTERED + FileTypes.GZIPPED_VCF)
                .build();
    }

    static GripssConfiguration germline() {
        return ImmutableGripssConfiguration.builder()
                .filteredVcf(m -> m.reference().sampleName() + GRIPSS_GERMLINE_FILTERED + FileTypes.GZIPPED_VCF)
                .unfilteredVcf(m -> m.reference().sampleName() + GRIPSS_GERMLINE_UNFILTERED + FileTypes.GZIPPED_VCF)
                .build();
    }
}