package com.hartwig.pipeline.transfer.google;

import java.util.concurrent.TimeUnit;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryHttpInitializerWrapper implements HttpRequestInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(RetryHttpInitializerWrapper.class);
    private final Credential wrappedCredential;
    private final Sleeper sleeper;

    RetryHttpInitializerWrapper(Credential wrappedCredential) {
        this(wrappedCredential, Sleeper.DEFAULT);
    }

    private RetryHttpInitializerWrapper(Credential wrappedCredential, Sleeper sleeper) {
        this.wrappedCredential = Preconditions.checkNotNull(wrappedCredential);
        this.sleeper = sleeper;
    }

    public void initialize(HttpRequest request) {
        request.setReadTimeout(Long.valueOf(TimeUnit.MINUTES.toMillis(2)).intValue());
        final HttpUnsuccessfulResponseHandler backoffHandler =
                new HttpBackOffUnsuccessfulResponseHandler(new ExponentialBackOff()).setSleeper(sleeper);
        request.setInterceptor(wrappedCredential);
        request.setUnsuccessfulResponseHandler((request1, response, supportsRetry) -> {
            if (wrappedCredential.handleResponse(request1, response, supportsRetry)) {
                return true;
            } else if (backoffHandler.handleResponse(request1, response, supportsRetry)) {
                LOG.info("Retrying " + request1.getUrl().toString());
                return true;
            } else {
                return false;
            }
        });
        request.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(new ExponentialBackOff()).setSleeper(sleeper));
    }
}