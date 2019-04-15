package com.hartwig.pipeline.execution.vm;

import org.apache.commons.lang.RandomStringUtils;

class DataFixture {
    static String randomStr() {
        return RandomStringUtils.randomAlphabetic(16);
    }
}
