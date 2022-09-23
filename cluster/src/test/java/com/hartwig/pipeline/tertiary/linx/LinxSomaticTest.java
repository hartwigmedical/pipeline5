package com.hartwig.pipeline.tertiary.linx;

import java.util.Collections;
import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class LinxSomaticTest extends TertiaryStageTest<LinxSomaticOutput> {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected List<String> expectedInputs() {
        return Collections.singletonList(input("run-reference-tumor-test/purple/results/", "results"));
    }

    @Override
    protected Stage<LinxSomaticOutput, SomaticRunMetadata> createVictim() {
        return new LinxSomatic(TestInputs.purpleOutput(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<String> expectedCommands() {

        List<String> commands = Lists.newArrayList();

        commands.add("java -Xmx8G -jar /opt/tools/linx/1.22/linx.jar -sample tumor -sv_vcf "
                + "/data/input/tumor.purple.sv.vcf.gz -purple_dir /data/input/results -ref_genome_version V37 -output_dir /data/output "
                + "-fragile_site_file /opt/resources/linx/37/fragile_sites_hmf.37.csv "
                + "-line_element_file /opt/resources/linx/37/line_elements.37.csv "
                + "-ensembl_data_dir /opt/resources/ensembl_data_cache/37/ "
                + "-check_fusions -known_fusion_file /opt/resources/fusions/37/known_fusion_data.37.csv "
                + "-check_drivers -driver_gene_panel /opt/resources/gene_panel/37/DriverGenePanel.37.tsv " + "-write_vis_data");

        commands.add("java -Xmx8G -cp /opt/tools/linx/1.22/linx.jar com.hartwig.hmftools.linx.visualiser.SvVisualiser "
                + "-sample tumor -ref_genome_version V37 -circos /opt/tools/circos/0.69.6/bin/circos -vis_file_dir /data/output "
                + "-data_out /data/output/circos/ -plot_out /data/output/plot/ -plot_reportable");

        return commands;
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.LINX,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), LinxSomatic.NAMESPACE, "tumor.linx.drivers.tsv")),
                new AddDatatype(DataType.LINX_BREAKENDS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), LinxSomatic.NAMESPACE, "tumor.linx.breakend.tsv")),
                new AddDatatype(DataType.LINX_DRIVER_CATALOG,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), LinxSomatic.NAMESPACE, "tumor.linx.driver.catalog.tsv")),
                new AddDatatype(DataType.LINX_DRIVERS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), LinxSomatic.NAMESPACE, "tumor.linx.drivers.tsv")),
                new AddDatatype(DataType.LINX_SV_ANNOTATIONS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), LinxSomatic.NAMESPACE, "tumor.linx.svs.tsv")),
                new AddDatatype(DataType.LINX_CLUSTERS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), LinxSomatic.NAMESPACE, "tumor.linx.clusters.tsv")),
                new AddDatatype(DataType.LINX_FUSIONS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), LinxSomatic.NAMESPACE, "tumor.linx.fusion.tsv")));
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected void validateOutput(final LinxSomaticOutput output) {
        // no additional validation
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final LinxSomaticOutput output) {
        // no additional validation
    }

    @Override
    protected void validatePersistedOutput(final LinxSomaticOutput output) {
        // no additional validation
    }

    @Override
    public void addsLogs() {
        // not supported currently
    }
}