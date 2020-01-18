package com.hartwig.bcl2fastq;

import com.hartwig.bcl2fastq.conversion.Conversion;
import com.hartwig.bcl2fastq.conversion.ConvertedFastq;
import com.hartwig.bcl2fastq.conversion.ConvertedSample;
import com.hartwig.pipeline.storage.GsUtilFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static com.hartwig.pipeline.storage.GsUtilFacade.GsCopyOption.NO_CLOBBER;
import static java.lang.String.format;

public class OutputArchiver implements Consumer<Conversion> {

    private final static Logger LOGGER = LoggerFactory.getLogger(OutputArchiver.class);
    private final Bcl2fastqArguments args;
    private GsUtilFacade gsUtil;

    public OutputArchiver(Bcl2fastqArguments arguments, GsUtilFacade gsUtil) {
        this.args = arguments;
        this.gsUtil = gsUtil;
    }

    @Override
    public void accept(Conversion conversion) {
        LOGGER.info("Starting transfer from [{}] to GCP bucket [{}]", args.outputBucket(), args.archiveBucket());
        try {
            for (ConvertedSample sample : conversion.samples()) {
                for (ConvertedFastq fastq : sample.fastq()) {
                    String dest = format("gs://%s", args.archiveBucket());
                    gsUtil.copy(format("gs://%s/%s", args.outputBucket(), fastq.outputPathR1()), dest, NO_CLOBBER);
                    gsUtil.copy(format("gs://%s/%s", args.outputBucket(), fastq.outputPathR2()), dest, NO_CLOBBER);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOGGER.info("Transfer complete.");
    }
}
