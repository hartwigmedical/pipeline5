package com.hartwig.pipeline.calling.germline;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

public class CombineFilteredVariantsTest extends SubStageTest{

    @Override
    public SubStage createVictim() {
        return new CombineFilteredVariants("other.vcf");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.filtered_variants.vcf";
    }
}