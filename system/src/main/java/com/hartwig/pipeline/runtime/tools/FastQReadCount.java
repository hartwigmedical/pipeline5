package com.hartwig.pipeline.runtime.tools;

import java.text.NumberFormat;
import java.util.HashMap;

import com.hartwig.pipeline.runtime.spark.SparkContexts;

import org.apache.spark.api.java.JavaSparkContext;
import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FastQReadCount {

    private final Logger LOGGER = LoggerFactory.getLogger(FastQReadCount.class);
    private final String fastqTemplate;

    private FastQReadCount(final String fastqTemplate) {
        this.fastqTemplate = fastqTemplate;
    }

    public void execute() {
        JavaSparkContext context = SparkContexts.create("fastq", null, new HashMap<>());
        JavaADAMContext adamContext = new JavaADAMContext(new ADAMContext(context.sc()));

        for (int i = 1; i < 9; i++) {
            double bases = adamContext.loadAlignments(String.format(fastqTemplate, i)).toFragments().rdd().count();

            LOGGER.info("Paired reads in lane [{}] is [{}]", i, NumberFormat.getNumberInstance().format(bases));
        }
    }

    public static void main(String[] args) {
        new FastQReadCount("gs://run-colo829r-validation/samples/COLO829R/COLO829R_AHCT3FCCXY_S2_L00%s_R**.fastq").execute();
    }
}