package com.hartwig.pipeline.tertiary.linx;

import com.google.api.client.util.Lists;
import com.hartwig.computeengine.input.SomaticRunMetadata;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;
import org.junit.Before;

import java.util.Collections;
import java.util.List;

import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.tools.HmfTool.LINX;

public class LinxGermlineTest extends TertiaryStageTest<LinxGermlineOutput> {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected List<String> expectedInputs() {
        return Collections.singletonList(input(
                "run-reference-tumor-test/purple/tumor.purple.sv.germline.vcf.gz",
                "tumor.purple.sv.germline.vcf.gz"));
    }

    @Override
    protected Stage<LinxGermlineOutput, SomaticRunMetadata> createVictim() {
        return new LinxGermline(TestInputs.purpleOutput(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<String> expectedCommands() {

        List<String> commands = Lists.newArrayList();

        commands.add(
                toolCommand(LINX)
                        + " -sample tumor -germline "
                        + "-sv_vcf /data/input/tumor.purple.sv.germline.vcf.gz "
                        + "-ref_genome_version V37 "
                        + "-output_dir /data/output "
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
                new AddDatatype(DataType.LINX_GERMLINE_BREAKENDS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), LinxGermline.NAMESPACE, "tumor.linx.germline.breakend.tsv")),
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