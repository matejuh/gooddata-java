/**
 * Copyright (C) 2004-2016, GoodData(R) Corporation. All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE.txt file in the root directory of this source tree.
 */
package com.gooddata;

import static com.gooddata.util.Validate.notNull;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.with;
import static org.springframework.http.HttpMethod.GET;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.awaitility.core.ConditionTimeoutException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.client.HttpMessageConverterExtractor;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Parent for GoodData services providing helpers for REST API calls and polling.
 */
public abstract class AbstractService {

    public static final Integer WAIT_BEFORE_RETRY_IN_MILLIS = 5 * 1000;

    protected final RestTemplate restTemplate;

    protected final ObjectMapper mapper = new ObjectMapper();

    private final ResponseExtractor<ClientHttpResponse> reusableResponseExtractor = ReusableClientHttpResponse::new;


    /**
     * Sets RESTful HTTP Spring template. Should be called from constructor of concrete service extending
     * this abstract one.
     *
     * @param restTemplate RESTful HTTP Spring template
     */
    public AbstractService(RestTemplate restTemplate) {
        this.restTemplate = notNull(restTemplate, "restTemplate");
    }

    final <R> R poll(final PollHandler<?,R> handler, long timeout, final TimeUnit unit) {
        notNull(handler, "handler");

        try {
            with().pollInterval(WAIT_BEFORE_RETRY_IN_MILLIS, MILLISECONDS).await().atMost(timeout, unit).until(
                    () -> pollOnce(handler)
            );
        } catch (ConditionTimeoutException e) {
            throw new GoodDataException("timeout");
        }
        return handler.getResult();
    }

    final <P> boolean pollOnce(final PollHandler<P,?> handler) {
        notNull(handler, "handler");

        final ClientHttpResponse response;
        try {
            response = restTemplate.execute(handler.getPollingUri(), GET, null, reusableResponseExtractor);
        } catch (GoodDataRestException e) {
            handler.handlePollException(e);
            throw new GoodDataException("Handler " + handler.getClass().getName() + " didn't handle exception", e);
        }
        try {
            if (handler.isFinished(response)) {
                final P data = extractData(response, handler.getPollClass());
                handler.handlePollResult(data);
            } else if (HttpStatus.Series.CLIENT_ERROR.equals(response.getStatusCode().series())) {
                throw new GoodDataException(
                        format("Polling returned client error HTTP status %s", response.getStatusCode().value())
                );
            }
        } catch (IOException e) {
            throw new GoodDataException("I/O error occurred during HTTP response extraction", e);
        }
        return handler.isDone();
    }

    protected final <T> T extractData(ClientHttpResponse response, Class<T> cls) throws IOException {
        notNull(response, "response");
        notNull(cls, "cls");
        if (Void.class.isAssignableFrom(cls)) {
            return null;
        }
        return new HttpMessageConverterExtractor<>(cls, restTemplate.getMessageConverters()).extractData(response);
    }

    private static class ReusableClientHttpResponse implements ClientHttpResponse {

        private byte[] body;
        private final HttpStatus statusCode;
        private final int rawStatusCode;
        private final String statusText;
        private final HttpHeaders headers;

        public ReusableClientHttpResponse(ClientHttpResponse response) {
            try {
                final InputStream bodyStream = response.getBody();
                if (bodyStream != null) {
                    body = FileCopyUtils.copyToByteArray(bodyStream);
                }
                statusCode = response.getStatusCode();
                rawStatusCode = response.getRawStatusCode();
                statusText = response.getStatusText();
                headers = response.getHeaders();
            } catch (IOException e) {
                throw new RuntimeException("Unable to read from HTTP response", e);
            } finally {
                if (response != null) {
                    response.close();
                }
            }
        }

        @Override
        public HttpStatus getStatusCode() throws IOException {
            return statusCode;
        }

        @Override
        public int getRawStatusCode() throws IOException {
            return rawStatusCode;
        }

        @Override
        public String getStatusText() throws IOException {
            return statusText;
        }

        @Override
        public HttpHeaders getHeaders() {
            return headers;
        }

        @Override
        public InputStream getBody() throws IOException {
            return body != null ? new ByteArrayInputStream(body) : null;
        }

        @Override
        public void close() {
            //already closed
        }
    }

    protected static class OutputStreamResponseExtractor implements ResponseExtractor<Integer> {
        private final OutputStream output;

        public OutputStreamResponseExtractor(OutputStream output) {
            this.output = output;
        }

        @Override
        public Integer extractData(ClientHttpResponse response) throws IOException {
            return FileCopyUtils.copy(response.getBody(), output);
        }
    }

}
