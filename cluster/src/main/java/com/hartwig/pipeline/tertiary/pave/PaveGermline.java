package com.hartwig.pipeline.tertiary.pave;

import com.hartwig.pipeline.calling.sage.SageGermlinePostProcess;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.resource.ResourceFiles;

public class PaveGermline extends Pave
{
    public static final String NAMESPACE = "pave_germline";

    public PaveGermline(final ResourceFiles resourceFiles, SageOutput sageOutput, final PersistedDataset persistedDataset)
    {
        super(resourceFiles, sageOutput, persistedDataset, DataType.GERMLINE_VARIANTS_PAVE);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    protected String outputFile(final SomaticRunMetadata metadata)
    {
        return String.format("%s.%s.%s.%s",
                metadata.tumor().sampleName(),
                SageGermlinePostProcess.SAGE_GERMLINE_FILTERED,
                PAVE_FILE_ID,
                FileTypes.GZIPPED_VCF);
    }

}
