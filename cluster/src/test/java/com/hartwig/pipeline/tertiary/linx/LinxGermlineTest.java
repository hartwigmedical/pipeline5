package com.hartwig.pipeline.tertiary.linx;

import java.util.Collections;
import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class LinxGermlineTest extends TertiaryStageTest<LinxGermlineOutput> {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected List<String> expectedInputs() {
        return Collections.singletonList(input(
                "run-reference-tumor-test/gripss_germline/results/tumor.gripss.filtered.vcf.gz",
                    "tumor.gripss.filtered.vcf.gz"));
    }

    @Override
    protected Stage<LinxGermlineOutput, SomaticRunMetadata> createVictim() {
        return new LinxGermline(TestInputs.gripssGermlineOutput(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<String> expectedCommands() {

        List<String> commands = Lists.newArrayList();

        commands.add("java -Xmx8G -jar /opt/tools/linx/1.22/linx.jar -sample tumor -germline "
                + "-sv_vcf /data/input/tumor.gripss.filtered.vcf.gz -ref_genome_version V37 -output_dir /data/output "
                + "-line_element_file /opt/resources/linx/37/line_elements.37.csv "
                + "-ensembl_data_dir /opt/resources/ensembl_data_cache/37/ "
                + "-driver_gene_panel /opt/resources/gene_panel/37/DriverGenePanel.37.tsv");

        return commands;
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(
                new AddDatatype(DataType.LINX_GERMLINE_DISRUPTIONS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), LinxGermline.NAMESPACE, "tumor.linx.germline.disruption.tsv")),
                new AddDatatype(DataType.LINX_GERMLINE_DRIVER_CATALOG,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), LinxGermline.NAMESPACE, "tumor.linx.germline.driver.catalog.tsv")));
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected void validateOutput(final LinxGermlineOutput output) {
        // no additional validation
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final LinxGermlineOutput output) {
        // no additional validation
    }

    @Override
    protected void validatePersistedOutput(final LinxGermlineOutput output) {
        // no additional validation
    }

    @Override
    public void addsLogs() {
        // not supported currently
    }
}