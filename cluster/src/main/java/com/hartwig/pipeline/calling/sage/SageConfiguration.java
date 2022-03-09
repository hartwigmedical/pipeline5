package com.hartwig.pipeline.calling.sage;

import java.util.function.BiFunction;
import java.util.function.Function;

import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;

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

    OutputTemplate unfilteredTemplate();

    OutputTemplate filteredTemplate();

    OutputTemplate geneCoverageTemplate();

    SageCommandBuilder commandBuilder();

    Function<SomaticRunMetadata, SubStage> postProcess();

    BiFunction<BashStartupScript, ResultsDirectory, VirtualMachineJobDefinition> jobDefinition();

    static SageConfiguration germline(final ResourceFiles resourceFiles) {
        return ImmutableSageConfiguration.builder()
                .namespace(SAGE_GERMLINE_NAMESPACE)
                .vcfDatatype(DataType.GERMLINE_VARIANTS_SAGE)
                .geneCoverageDatatype(DataType.GERMLINE_GENE_COVERAGE)
                .tumorSampleBqrPlot(DataType.GERMLINE_TUMOR_SAMPLE_BQR_PLOT)
                .refSampleBqrPlot(DataType.GERMLINE_REF_SAMPLE_BQR_PLOT)
                .filteredTemplate(m -> String.format("%s.%s.%s",
                        m.tumor().sampleName(),
                        SageGermlinePostProcess.SAGE_GERMLINE_FILTERED,
                        FileTypes.GZIPPED_VCF))
                .unfilteredTemplate(m -> String.format("%s.%s.%s", m.tumor().sampleName(), "sage.germline", FileTypes.GZIPPED_VCF))
                .geneCoverageTemplate(m -> String.format("%s.%s", m.reference().sampleName(), SageCaller.SAGE_GENE_COVERAGE_TSV))
                .commandBuilder(new SageCommandBuilder(resourceFiles).germlineMode().addCoverage().maxHeap("15G"))
                .postProcess(m -> new SageGermlinePostProcess(m.reference().sampleName(), m.tumor().sampleName(), resourceFiles))
                .jobDefinition(VirtualMachineJobDefinition::sageGermlineCalling)
                .build();
    }

    static SageConfiguration somatic(final ResourceFiles resourceFiles, final boolean shallow) {
        return ImmutableSageConfiguration.builder()
                .namespace(SAGE_SOMATIC_NAMESPACE)
                .vcfDatatype(DataType.SOMATIC_VARIANTS_SAGE)
                .geneCoverageDatatype(DataType.SOMATIC_GENE_COVERAGE)
                .tumorSampleBqrPlot(DataType.SOMATIC_TUMOR_SAMPLE_BQR_PLOT)
                .refSampleBqrPlot(DataType.SOMATIC_REF_SAMPLE_BQR_PLOT)
                .filteredTemplate(m -> String.format("%s.%s.%s",
                        m.tumor().sampleName(),
                        SageSomaticPostProcess.SAGE_SOMATIC_FILTERED,
                        FileTypes.GZIPPED_VCF))
                .unfilteredTemplate(m -> String.format("%s.%s.%s", m.tumor().sampleName(), "sage.somatic", FileTypes.GZIPPED_VCF))
                .geneCoverageTemplate(m -> String.format("%s.%s", m.tumor().sampleName(), SageCaller.SAGE_GENE_COVERAGE_TSV))
                .commandBuilder(new SageCommandBuilder(resourceFiles).shallowMode(shallow).addCoverage())
                .postProcess(m -> new SageSomaticPostProcess(m.tumor().sampleName(), resourceFiles))
                .jobDefinition(VirtualMachineJobDefinition::sageSomaticCalling)
                .build();
    }
}