/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.http;

import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.exception.MantaChecksumFailedException;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.http.entity.DigestedEntity;
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

/**
 * Helper class used for common HTTP operations against the Manta server.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class StandardHttpHelper implements HttpHelper {
    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardHttpHelper.class);

    /**
     * Reference to the Apache HTTP Client HTTP request creation class.
     */
    private final MantaConnectionFactory connectionFactory;

    /**
     * Current connection context used for maintaining state between requests.
     */
    private final MantaConnectionContext connectionContext;

    /**
     * Flag toggling the checksum verification of uploaded files.
     */
    private final boolean validateUploads;

    /**
     * Creates a new instance of the helper class.
     *
     * @param connectionContext saved context used between requests to the Manta client
     * @param connectionFactory instance used for building requests to Manta
     * @param config configuration context object
     */
    public StandardHttpHelper(final MantaConnectionContext connectionContext,
                              final MantaConnectionFactory connectionFactory,
                              final ConfigContext config) {
        this.connectionContext = connectionContext;
        this.connectionFactory = connectionFactory;
        this.validateUploads = ObjectUtils.firstNonNull(config.verifyUploads(),
                DefaultsConfigContext.DEFAULT_VERIFY_UPLOADS);
    }

    @Override
    public HttpResponse httpHead(final String path) throws IOException {
        Validate.notNull(path, "Path must not be null");

        LOGGER.debug("HEAD   {}", path);

        HttpHead head = connectionFactory.head(path);
        return executeAndCloseRequest(head, "HEAD   {} response [{}] {} ");
    }


    @Override
    public HttpResponse httpGet(final String path) throws IOException {
        Validate.notNull(path, "Path must not be null");

        LOGGER.debug("GET    {}", path);

        final HttpGet get = connectionFactory.get(path);
        return executeAndCloseRequest(get, "GET    {} response [{}] {} ");
    }


    @Override
    public HttpResponse httpDelete(final String path) throws IOException {
        Validate.notNull(path, "Path must not be null");

        LOGGER.debug("DELETE {}", path);

        final HttpDelete delete = connectionFactory.delete(path);
        return executeAndCloseRequest(delete, "DELETE {} response [{}] {} ");
    }


    @Override
    public HttpResponse httpPost(final String path) throws IOException {
        return httpPost(path, null, null);
    }


    @Override
    public HttpResponse httpPost(final String path,
                                 final MantaHttpHeaders headers,
                                 final HttpEntity entity) throws IOException {
        Validate.notNull(path, "Path must not be null");

        LOGGER.debug("POST   {}", path);

        final MantaHttpHeaders httpHeaders;

        if (headers == null) {
            httpHeaders = new MantaHttpHeaders();
        } else {
            httpHeaders = headers;
        }

        final HttpPost post = connectionFactory.post(path);
        post.setHeaders(httpHeaders.asApacheHttpHeaders());

        if (entity != null) {
            post.setEntity(entity);
        }

        return executeAndCloseRequest(post, (Integer)null,
                "POST   {} response [{}] {} ");
    }


    @Override
    public MantaObjectResponse httpPut(final String path,
                                       final MantaHttpHeaders headers,
                                       final HttpEntity entity,
                                       final MantaMetadata metadata)
            throws IOException {
        Validate.notNull(path, "Path must not be null");

        LOGGER.debug("PUT    {}", path);

        final MantaHttpHeaders httpHeaders;

        if (headers == null) {
            httpHeaders = new MantaHttpHeaders();
        } else {
            httpHeaders = headers;
        }

        if (metadata != null) {
            httpHeaders.putAll(metadata);
        }

        final HttpPut put = connectionFactory.put(path);
        put.setHeaders(httpHeaders.asApacheHttpHeaders());

        final DigestedEntity md5DigestedEntity;

        if (entity != null) {
            md5DigestedEntity = new DigestedEntity(entity, MessageDigestAlgorithms.MD5);
            put.setEntity(md5DigestedEntity);
        } else {
            md5DigestedEntity = null;
        }

        CloseableHttpClient client = connectionContext.getHttpClient();

        try (CloseableHttpResponse response = client.execute(put)) {
            StatusLine statusLine = response.getStatusLine();
            LOGGER.debug("PUT    {} response [{}] {} ", path, statusLine.getStatusCode(),
                    statusLine.getReasonPhrase());
            final MantaHttpHeaders responseHeaders = new MantaHttpHeaders(response.getAllHeaders());
            // We add back in the metadata made in the request so that it is easily available
            responseHeaders.putAll(httpHeaders.metadata());

            MantaObjectResponse obj = new MantaObjectResponse(path, responseHeaders, metadata);

            if (statusLine.getStatusCode() != HttpStatus.SC_NO_CONTENT) {
                throw new MantaClientHttpResponseException(put, response,
                        put.getURI().getPath());
            }

            /* We set the content type on the result object from the entity
             * PUT if that content type isn't already present on the result object.
             * This allows for the result object to have the original
             * content-type even if it isn't part of any response headers. */
            if (obj.getContentType() == null && entity != null && entity.getContentType() != null) {
                obj.setContentType(entity.getContentType().getValue());
            }

            if (validateUploadsEnabled()) {
                validateChecksum(md5DigestedEntity, obj.getMd5Bytes());
            }

            return obj;
        }
    }

    /**
     * Checks to make sure that the uploaded entity's MD5 matches the MD5 as
     * calculated on the server. This check is skipped if the entity is null.
     *
     * @param entity null or the entity object
     * @param serverMd5 service side computed MD5 value
     * @throws MantaChecksumFailedException thrown if the MD5 values do not match
     */
    protected static void validateChecksum(final DigestedEntity entity,
                                         final byte[] serverMd5)
            throws MantaChecksumFailedException {
        if (entity == null) {
            return;
        }

        if (serverMd5 == null || serverMd5.length == 0) {
            LOGGER.warn("No cryptographic check performed by the server");
            return;
        }

        final byte[] clientMd5 = entity.getDigest();
        final boolean areMd5sTheSame = Arrays.equals(serverMd5, clientMd5);

        if (!areMd5sTheSame) {
            String msg = "Client calculated MD5 and server calculated "
                    + "MD5 do not match";
            MantaChecksumFailedException e = new MantaChecksumFailedException(msg);
            e.setContextValue("serverMd5", MantaUtils.byteArrayAsHexString(serverMd5));
            e.setContextValue("clientMd5", MantaUtils.byteArrayAsHexString(clientMd5));

            throw e;
        }
    }

    @Override
    public CloseableHttpResponse executeRequest(final HttpUriRequest request,
                                                final String logMessage,
                                                final Object... logParameters)
            throws IOException {
        return executeRequest(request, null, logMessage, logParameters);
    }

    @Override
    public CloseableHttpResponse executeRequest(final HttpUriRequest request,
                                                final Integer expectedStatusCode,
                                                final String logMessage,
                                                final Object... logParameters)
            throws IOException {
        Validate.notNull(request, "Request object must not be null");

        CloseableHttpClient client = connectionContext.getHttpClient();

        CloseableHttpResponse response = client.execute(request);
        StatusLine statusLine = response.getStatusLine();

        if (LOGGER.isDebugEnabled() && logMessage != null) {
            LOGGER.debug(logMessage, logParameters, statusLine.getStatusCode(),
                    statusLine.getReasonPhrase());
        }

        if (isFailedStatusCode(expectedStatusCode, statusLine)) {
            throw new MantaClientHttpResponseException(request, response,
                    request.getURI().getPath());
        }

        return response;
    }

    @Override
    public CloseableHttpResponse executeAndCloseRequest(final HttpUriRequest request,
                                                        final String logMessage,
                                                        final Object... logParameters)
            throws IOException {
        return executeAndCloseRequest(request, (Integer)null, logMessage, logParameters);
    }

    @Override
    public CloseableHttpResponse executeAndCloseRequest(final HttpUriRequest request,
                                                        final Integer expectedStatusCode,
                                                        final String logMessage,
                                                        final Object... logParameters)
            throws IOException {
        return executeRequest(request, expectedStatusCode, true,
                logMessage, logParameters);
    }

    @Override
    public CloseableHttpResponse executeRequest(final HttpUriRequest request,
                                                final Integer expectedStatusCode,
                                                final boolean closeResponse,
                                                final String logMessage,
                                                final Object... logParameters)
            throws IOException {
        Validate.notNull(request, "Request object must not be null");

        CloseableHttpClient client = connectionContext.getHttpClient();
        CloseableHttpResponse response = client.execute(request);

        try {
            StatusLine statusLine = response.getStatusLine();

            if (LOGGER.isDebugEnabled() && logMessage != null) {
                LOGGER.debug(logMessage, logParameters, statusLine.getStatusCode(),
                        statusLine.getReasonPhrase());
            }

            if (isFailedStatusCode(expectedStatusCode, statusLine)) {
                String path = request.getURI().getPath();
                throw new MantaClientHttpResponseException(request, response,
                        path);
            }

            return response;
        } finally {
            if (closeResponse) {
                IOUtils.closeQuietly(response);
            }
        }
    }

    @Override
    public <R> R executeAndCloseRequest(final HttpUriRequest request,
                                        final Function<CloseableHttpResponse, R> responseAction,
                                        final String logMessage,
                                        final Object... logParameters)
            throws IOException {
        return executeAndCloseRequest(request, null,
                responseAction, logMessage, logParameters);
    }

    @Override
    public <R> R executeAndCloseRequest(final HttpUriRequest request,
                                        final Integer expectedStatusCode,
                                        final Function<CloseableHttpResponse, R> responseAction,
                                        final String logMessage,
                                        final Object... logParameters)
            throws IOException {
        return executeRequest(request, expectedStatusCode, responseAction,
                true, logMessage, logParameters);
    }

    @Override
    public <R> R executeRequest(final HttpUriRequest request,
                                   final Integer expectedStatusCode,
                                   final Function<CloseableHttpResponse, R> responseAction,
                                   final boolean closeResponse,
                                   final String logMessage,
                                   final Object... logParameters)
            throws IOException {
        Validate.notNull(request, "Request object must not be null");

        CloseableHttpClient client = connectionContext.getHttpClient();

        CloseableHttpResponse response = client.execute(request);
        try {
            StatusLine statusLine = response.getStatusLine();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(logMessage, logParameters, statusLine.getStatusCode(),
                        statusLine.getReasonPhrase());
            }

            if (isFailedStatusCode(expectedStatusCode, statusLine)) {
                throw new MantaClientHttpResponseException(request, response,
                        request.getURI().getPath());
            }

            if (responseAction != null) {
                return responseAction.apply(response);
            } else {
                return null;
            }
        } finally {
            if (closeResponse) {
                IOUtils.closeQuietly(response);
            }
        }
    }

    /**
     * Utility method that determines if a request failed by comparing the
     * status code to an expectation if present (non-null) or by finding out
     * if the HTTP status code is less than 400.
     *
     * @param expectedStatusCode null for default behavior or the specific status code
     * @param statusLine status line object containing status code
     * @return boolean true indicates the request failed
     */
    protected static boolean isFailedStatusCode(final Integer expectedStatusCode,
                                              final StatusLine statusLine) {
        int code = statusLine.getStatusCode();

        if (expectedStatusCode == null) {
            return code >= HttpStatus.SC_BAD_REQUEST;
        } else {
            return code != expectedStatusCode;
        }
    }

    protected MantaConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    protected MantaConnectionContext getConnectionContext() {
        return connectionContext;
    }

    /**
     * @return true if we are validating MD5 checksums against the Manta Computed-MD5 header
     */
    protected boolean validateUploadsEnabled() {
        return validateUploads;
    }

    @Override
    public void close() throws Exception {
        connectionContext.close();
    }
}