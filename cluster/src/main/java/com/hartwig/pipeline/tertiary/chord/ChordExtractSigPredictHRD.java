package com.hartwig.pipeline.tertiary.chord;

import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.r.RscriptCommand;
import com.hartwig.pipeline.resource.RefGenomeVersion;

import java.util.List;

import static com.hartwig.pipeline.tools.HmfTool.CHORD;

class ChordExtractSigPredictHRD extends RscriptCommand {

    ChordExtractSigPredictHRD(final String sampleName, final String somaticVcfPath, final String structuralVcfPath,
                              final RefGenomeVersion refGenomeVersion) {
        super(CHORD.getToolName(),
                CHORD.runVersion(),
                "extractSigPredictHRD.R",
                List.of(VmDirectories.TOOLS + "/chord/" + CHORD.runVersion(),
                        VmDirectories.OUTPUT,
                        sampleName,
                        somaticVcfPath,
                        structuralVcfPath,
                        refGenomeVersion.alphaNumeric())
        );
    }
}
