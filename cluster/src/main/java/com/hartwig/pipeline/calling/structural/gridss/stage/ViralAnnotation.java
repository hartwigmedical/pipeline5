package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import static com.hartwig.pipeline.execution.vm.VmDirectories.outputFile;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateUntemplatedSequence;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.unix.CpCommand;
import com.hartwig.pipeline.execution.vm.unix.GunzipAndKeepArchiveCommand;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;
import com.hartwig.pipeline.tools.Versions;

public class ViralAnnotation extends SubStage {

    private final String referenceGenome;

    public ViralAnnotation(final String referenceGenome) {
        super("viral_annotation", OutputFile.VCF, false);
        this.referenceGenome = referenceGenome;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        String inputVcf = input.path().replaceAll("\\.gz$", "");
        String withBealn = outputFile(format("%s.withbealn.vcf", output.fileName()));
        String missingBealn = outputFile(format("%s.missingbealn.vcf", output.fileName()));
        String annotatedBealn = outputFile(format("%s.withannotation.vcf", output.fileName()));

        return bash.addCommand(new GunzipAndKeepArchiveCommand(input.path()))
                .addCommand(() -> format("grep -E '^#' %s > %s", inputVcf, withBealn))
                .addCommand(new CpCommand(withBealn, missingBealn))
                .addCommand(new PipeCommands(
                        () -> format("grep BEALN %s", inputVcf),
                        () -> format("grep -vE '^#' >> %s", withBealn)))
                .addCommand(new PipeCommands(
                        () -> format("grep -v BEALN %s", inputVcf),
                        () -> format("grep -vE '^#' >> %s", missingBealn)))
                .addCommand(new PipeCommands(
                        () -> format("candidates=$(grep -vE '^#' %s", missingBealn),
                        () -> format("grep -cE '([].[][ACGTNagctn]{2})|([ACGTNagctn]{2}[].[])')")))
                .addCommand(new AnnotateUntemplatedSequence(missingBealn, referenceGenome, annotatedBealn))
                .addCommand(new JavaJarCommand("picard", Versions.PICARD, "picard.jar", "2G",
                        asList("SortVcf", "I=" + withBealn, "I=" + annotatedBealn, "O=" + output.path())));
    }
}

/*

input_vcf=$1 && shift

PICARD_JAR=${base_path}/tools/picard-tools_v1.135/picard.jar
ref_genome=${base_path}/refgenomes/human_virus/human_virus.fa
output_vcf=${input_vcf/.vcf.gz/.ann.vcf.gz}
input_decompressed=${input_vcf/.gz/}
withBEALN=${output_vcf}.withbealn.vcf
missingBEALN=${output_vcf}.missingbealn.vcf
annotatedBEALN=${output_vcf}.withannotation.vcf

if [[ ! -f ${input_vcf} ]] ; then
        echo "[ERROR] Missing input VCF $input_vcf"
        exit 1
fi

if [[ "${input_vcf/.ann.vcf.gz/}" != "$input_vcf" ]] ; then
        echo "[WARN] Already annotated purple VCF $input_vcf. Exiting."
    exit 1
fi

gunzip -c ${input_vcf} > ${input_decompressed}
grep -E "^#" < ${input_decompressed} | tee ${withBEALN} > ${missingBEALN}
grep BEALN < ${input_decompressed}  | grep -vE "^#" >> ${withBEALN}
grep -v BEALN < ${input_decompressed}  | grep -vE "^#" >> ${missingBEALN}

# Work-around for https://github.com/PapenfussLab/gridss/issues/199
CANDIDATES=$(grep -vE "^#" < ${missingBEALN} | grep -E '([].[][ACGTNagctn]{2})|([ACGTNagctn]{2}[].[])' | wc -l)
if [[ ${CANDIDATES} -gt 0 ]] ; then
        java -Xmx1G ${gridss_jvm_args} \
                -Dgridss.output_to_temp_file=true \
                gridss.AnnotateUntemplatedSequence \
                REFERENCE_SEQUENCE=${ref_genome} \
                INPUT=${missingBEALN} \
                OUTPUT=${annotatedBEALN} \
                WORKER_THREADS=${threads}
else
        cp ${missingBEALN} ${annotatedBEALN}
fi

java -jar ${PICARD_JAR} SortVcf \
        I=${withBEALN} \
        I=${annotatedBEALN} \
        O=${output_vcf}

rm ${input_decompressed} ${withBEALN} ${missingBEALN} ${annotatedBEALN} ${annotatedBEALN}.idx

 */