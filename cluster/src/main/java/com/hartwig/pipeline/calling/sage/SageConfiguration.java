package com.hartwig.pipeline.calling.sage;

import java.util.function.Function;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.SubStage;

import org.immutables.value.Value;

@Value.Immutable
public interface SageConfiguration {

    String namespace();

    DataType vcfDatatype();

    DataType geneCoverageDatatype();

    DataType tumorSampleBqrPlot();

    DataType refSampleBqrPlot();

    OutputTemplate unfilteredTemplate();

    OutputTemplate filteredTemplate();

    SageCommandBuilder commandBuilder();

    Function<String, SubStage> postProcess();

    static SageConfiguration germline() {
        // fill in with all the germline specifics
        return null;
    }

    static SageConfiguration somatic() {
        // fill in with all the somatic specifics
        return null;
    }
}
