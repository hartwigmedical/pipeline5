package com.hartwig.pipeline.runtime;

import com.hartwig.pipeline.runtime.configuration.BwaParameters;
import com.hartwig.pipeline.runtime.configuration.Configuration;
import com.hartwig.pipeline.runtime.configuration.KnownIndelParameters;
import com.hartwig.pipeline.runtime.configuration.PatientParameters;
import com.hartwig.pipeline.runtime.configuration.PipelineParameters;
import com.hartwig.pipeline.runtime.configuration.ReferenceGenomeParameters;

public class GoogleCloudPipelineRuntime {

    public static void main(String[] args) {
        Configuration configuration = Configuration.builder()
                .pipeline(PipelineParameters.builder().hdfs("gs:///").bwa(BwaParameters.builder().threads(1).build()).build())
                .referenceGenome(ReferenceGenomeParameters.builder().file("reference.fasta").build())
                .knownIndel(KnownIndelParameters.builder()
                        .addFiles("1000G_phase1.indels.b37.vcf.gz", "Mills_and_1000G_gold_standard.indels.b37.vcf.gz")
                        .build()).patient(PatientParameters.builder().directory("/samples").name("").build())
                .build();
        new PipelineRuntime(configuration).start();
    }
}
