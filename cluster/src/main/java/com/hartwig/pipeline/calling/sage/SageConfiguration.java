package com.hartwig.pipeline.calling.sage;

import static com.hartwig.pipeline.calling.sage.SageApplication.SAGE_GERMLINE_VCF_ID;
import static com.hartwig.pipeline.calling.sage.SageApplication.SAGE_SOMATIC_VCF_ID;

import java.util.function.BiFunction;

import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
import com.hartwig.pipeline.resource.ResourceFiles;

import org.immutables.value.Value;

@Value.Immutable
public interface SageConfiguration {

    String SAGE_GERMLINE_NAMESPACE = "sage_germline";
    String SAGE_SOMATIC_NAMESPACE = "sage_somatic";

    String namespace();

    DataType vcfDatatype();

    DataType geneCoverageDatatype();

    DataType tumorSampleBqrPlot();

    DataType refSampleBqrPlot();

    OutputTemplate vcfFile();

    OutputTemplate geneCoverageTemplate();

    SageCommandBuilder commandBuilder();

    BiFunction<BashStartupScript, ResultsDirectory, VirtualMachineJobDefinition> jobDefinition();

    static SageConfiguration germline(final ResourceFiles resourceFiles) {
        return ImmutableSageConfiguration.builder().namespace(SAGE_GERMLINE_NAMESPACE)
                .vcfDatatype(DataType.GERMLINE_VARIANTS_SAGE)
                .geneCoverageDatatype(DataType.GERMLINE_GENE_COVERAGE)
                .tumorSampleBqrPlot(DataType.GERMLINE_TUMOR_SAMPLE_BQR_PLOT)
                .refSampleBqrPlot(DataType.GERMLINE_REF_SAMPLE_BQR_PLOT)
                .vcfFile(m -> String.format("%s.%s.%s", m.sampleName(), SAGE_GERMLINE_VCF_ID, FileTypes.GZIPPED_VCF))
                .geneCoverageTemplate(m -> String.format("%s.%s", m.reference().sampleName(), SageCaller.SAGE_GENE_COVERAGE_TSV))
                .commandBuilder(new SageCommandBuilder(resourceFiles).germlineMode())
                .jobDefinition(VirtualMachineJobDefinitions::sageGermlineCalling)
                .build();
    }

    static SageConfiguration somatic(final ResourceFiles resourceFiles, final Arguments arguments) {
        return ImmutableSageConfiguration.builder().namespace(SAGE_SOMATIC_NAMESPACE)
                .vcfDatatype(DataType.SOMATIC_VARIANTS_SAGE)
                .geneCoverageDatatype(DataType.SOMATIC_GENE_COVERAGE)
                .tumorSampleBqrPlot(DataType.SOMATIC_TUMOR_SAMPLE_BQR_PLOT)
                .refSampleBqrPlot(DataType.SOMATIC_REF_SAMPLE_BQR_PLOT)
                .vcfFile(m -> String.format("%s.%s.%s", m.tumor().sampleName(), SAGE_SOMATIC_VCF_ID, FileTypes.GZIPPED_VCF))
                .geneCoverageTemplate(m -> String.format("%s.%s", m.tumor().sampleName(), SageCaller.SAGE_GENE_COVERAGE_TSV))
                .commandBuilder(new SageCommandBuilder(resourceFiles).targetRegionsMode(arguments.useTargetRegions()))
                .jobDefinition(VirtualMachineJobDefinitions::sageSomaticCalling)
                .build();
    }
}
