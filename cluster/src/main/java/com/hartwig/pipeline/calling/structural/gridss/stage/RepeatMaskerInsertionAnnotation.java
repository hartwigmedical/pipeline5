package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.MvCommand;

public class RepeatMaskerInsertionAnnotation extends SubStage {
    private final String repeatMaskerDb;

    public RepeatMaskerInsertionAnnotation(final String repeatMaskerDb) {
        super("repeatmaster_annotation", OutputFile.GZIPPED_VCF);
        this.repeatMaskerDb = repeatMaskerDb;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        String initialOutputPath = format("%s/rscript_output", VmDirectories.OUTPUT);
        bash.addCommand(() -> format("Rscript %s --input %s --output %s --repeatmasker %s --scriptDir %s",
                "/.../gridss_annotate_insertions_repeatmaster.R", input.path(), initialOutputPath, repeatMaskerDb,
                ".../libgridss"));
        bash.addCommand(new MvCommand(initialOutputPath + ".bgz", output.fileName()));
        bash.addCommand(new MvCommand(initialOutputPath + ".bgz.tbi", output.path() + ".tbi"));
        return bash;
    }

    /*

#!/bin/bash
#
# Adds RepeatMasker annotations for non-template inserted sequences
#
input_vcf=$1 && shift

output_vcf=${input_vcf/.gz/}

if [[ ! -f ${input_vcf} ]] ; then
        echo "[ERROR] Missing input VCF $input_vcf"
        exit 1
fi

base_path="/data/common"
if [ -z "`hostname | grep datastore`" ]; then
        # Common folder does not exist on crunches...
        base_path="/data"
fi

repeatmasker=${base_path}/dbs/repeatmasker/hg19.fa.out
libgridss=/data/common/repos/scripts/gridss/

Rscript ${libgridss}/gridss_annotate_insertions_repeatmaster.R \
        --input ${input_vcf} \
        --output ${output_vcf} \
        --repeatmasker ${repeatmasker} \
        --scriptdir ${libgridss}

mv ${output_vcf}.bgz ${output_vcf}.gz
mv ${output_vcf}.bgz.tbi ${output_vcf}.gz.tbi

     */
}
