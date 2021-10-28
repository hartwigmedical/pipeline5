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

public class LinxTest extends TertiaryStageTest<LinxOutput> {

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
    protected Stage<LinxOutput, SomaticRunMetadata> createVictim() {
        return new Linx(TestInputs.purpleOutput(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<String> expectedCommands() {

        List<String> commands = Lists.newArrayList();

        commands.add("java -Xmx8G -jar /opt/tools/linx/1.16/linx.jar -sample tumor -sv_vcf "
                + "/data/input/tumor.purple.sv.vcf.gz -purple_dir /data/input/results -ref_genome_version 37 -output_dir /data/output "
                + "-fragile_site_file /opt/resources/linx/37/fragile_sites_hmf.37.csv "
                + "-line_element_file /opt/resources/linx/37/line_elements.37.csv "
                + "-replication_origins_file /opt/resources/linx/37/heli_rep_origins.37.bed "
                + "-viral_hosts_file /opt/resources/linx/viral_host_ref.csv "
                + "-gene_transcripts_dir /opt/resources/ensembl_data_cache/37/ "
                + "-check_fusions -known_fusion_file /opt/resources/fusions/37/known_fusion_data.37.csv "
                + "-check_drivers -driver_gene_panel /opt/resources/gene_panel/37/DriverGenePanel.37.tsv "
                + "-chaining_sv_limit 0 -write_vis_data");

        commands.add("java -Xmx8G -cp /opt/tools/linx/1.16/linx.jar com.hartwig.hmftools.linx.visualiser.SvVisualiser "
                + "-sample tumor -ref_genome_version 37 -circos /opt/tools/circos/0.69.6/bin/circos -vis_file_dir /data/output "
                + "-data_out /data/output/circos/ -plot_out /data/output/plot/ -plot_reportable");

        return commands;
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.LINX,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Linx.NAMESPACE, "tumor.linx.drivers.tsv")),
                new AddDatatype(DataType.LINX_BREAKENDS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Linx.NAMESPACE, "tumor.linx.breakend.tsv")),
                new AddDatatype(DataType.LINX_DRIVER_CATALOG,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Linx.NAMESPACE, "tumor.linx.driver.catalog.tsv")),
                new AddDatatype(DataType.LINX_FUSIONS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Linx.NAMESPACE, "tumor.linx.fusion.tsv")),
                new AddDatatype(DataType.LINX_VIRAL_INSERTS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Linx.NAMESPACE, "tumor.linx.viral_inserts.tsv")));
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected void validateOutput(final LinxOutput output) {
        // no additional validation
    }

    @Override
    public void addsLogs() {
        // not supported currently
    }
}