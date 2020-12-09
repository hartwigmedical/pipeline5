package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.stages.SubStage;

class PonFilter extends SubStage {

    private static final String PON_FILTER = "PON";
    private static final String HOTSPOT = "INFO/TIER=\"HOTSPOT\" && PON_MAX>=%s && PON_COUNT >= %s";
    private static final String PANEL = "INFO/TIER=\"PANEL\" && PON_MAX>=%s && PON_COUNT >= %s";
    private static final String OTHER = "INFO/TIER!=\"HOTSPOT\" && INFO/TIER!=\"PANEL\" && PON_COUNT >= %s";

    private final String panelFilter;
    private final String otherFilter;
    private final String hotspotFilter;

    PonFilter(RefGenomeVersion refGenomeVersion) {
        super("sage.pon.filter", FileTypes.GZIPPED_VCF);

        if (refGenomeVersion.equals(RefGenomeVersion.RG_37)) {
            hotspotFilter = String.format(HOTSPOT, 5, 10);
            panelFilter = String.format(PANEL, 5, 6);
            otherFilter = String.format(OTHER, 6);
        } else {
            hotspotFilter = String.format(HOTSPOT, 5, 5);
            panelFilter = String.format(PANEL, 5, 2);
            otherFilter = String.format(OTHER, 2);
        }
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex()
                .excludeSoftFilter("'PON_COUNT!=\".\" && " + hotspotFilter + "'", PON_FILTER)
                .excludeSoftFilter("'PON_COUNT!=\".\" && " + panelFilter + "'", PON_FILTER)
                .excludeSoftFilter("'PON_COUNT!=\".\" && " + otherFilter + "'", PON_FILTER)
                .build();
    }
}
