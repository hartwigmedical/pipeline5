package com.hartwig.bcl2fastq;

import com.hartwig.bcl2fastq.conversion.Conversion;
import com.hartwig.bcl2fastq.conversion.ConvertedFastq;
import com.hartwig.bcl2fastq.conversion.ConvertedSample;
import com.hartwig.pipeline.storage.GsUtilFacade;
import com.hartwig.pipeline.storage.RuntimeBucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static java.lang.String.format;

public class OutputCopier implements Consumer<Conversion> {

    private final static Logger LOGGER = LoggerFactory.getLogger(OutputCopier.class);
    private final Bcl2fastqArguments args;
    private RuntimeBucket runtimeBucket;
    private GsUtilFacade gsUtil;

    public OutputCopier(Bcl2fastqArguments arguments, RuntimeBucket runtimeBucket, GsUtilFacade gsUtil) {
        this.args = arguments;
        this.runtimeBucket = runtimeBucket;
        this.gsUtil = gsUtil;
    }

    @Override
    public void accept(Conversion conversion) {
        LOGGER.info("Starting transfer from [{}] to GCP bucket [{}]", runtimeBucket.getUnderlyingBucket(),
                args.outputBucket());
        try {
            for (ConvertedSample sample : conversion.samples()) {
                for (ConvertedFastq fastq : sample.fastq()) {
                    copy(fastq.pathR1(), fastq.outputPathR1());
                    copy(fastq.pathR2(), fastq.outputPathR2());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOGGER.info("Transfer complete.");
    }

    private void copy(String sourcePath, String destPath) {
        gsUtil.copy(format("gs://%s/%s", runtimeBucket.getUnderlyingBucket().getName(), sourcePath),
                format("gs://%s/%s", args.outputBucket(), destPath));
    }
}
