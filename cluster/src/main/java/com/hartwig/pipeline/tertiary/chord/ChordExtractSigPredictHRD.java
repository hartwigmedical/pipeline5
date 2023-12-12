package com.hartwig.pipeline.tertiary.chord;

import static com.hartwig.pipeline.tools.HmfTool.CHORD;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.RefGenomeVersion;

class ChordExtractSigPredictHRD extends VersionedToolCommand {

    ChordExtractSigPredictHRD(final String sampleName, final String somaticVcfPath, final String structuralVcfPath,
            final RefGenomeVersion refGenomeVersion) {
        super(CHORD.getToolName(),
                "extractSigPredictHRD.R",
                CHORD.runVersion(),
                VmDirectories.TOOLS + "/chord/" + CHORD.runVersion(),
                VmDirectories.OUTPUT,
                sampleName,
                somaticVcfPath,
                structuralVcfPath,
                refGenomeVersion.alphaNumeric());
    }
}
