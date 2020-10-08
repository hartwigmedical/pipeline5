package com.hartwig.pipeline.tertiary.chord;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.tools.Versions;

class ChordExtractSigPredictHRD extends VersionedToolCommand {

    ChordExtractSigPredictHRD(final String sampleName, final String somaticVcfPath, final String structuralVcfPath,
            final RefGenomeVersion refGenomeVersion) {
        super("chord",
                "extractSigPredictHRD.R",
                Versions.CHORD,
                VmDirectories.TOOLS + "/chord/" + Versions.CHORD,
                VmDirectories.OUTPUT,
                sampleName,
                somaticVcfPath,
                structuralVcfPath,
                refGenomeVersion.toString());
    }
}
