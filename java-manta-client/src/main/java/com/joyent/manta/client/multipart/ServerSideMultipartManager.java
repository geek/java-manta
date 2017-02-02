package com.joyent.manta.client.multipart;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectMapper;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.HttpHelper;
import com.joyent.manta.http.MantaConnectionContext;
import com.joyent.manta.http.MantaConnectionFactory;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.http.entity.ExposedByteArrayEntity;
import com.joyent.manta.http.entity.ExposedStringEntity;
import com.joyent.manta.http.entity.MantaInputStreamEntity;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.exception.ExceptionContext;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.joyent.manta.client.MantaClient.SEPARATOR;

/**
 * Class providing a server-side natively supported implementation
 * of multipart uploads.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class ServerSideMultipartManager
        implements MantaMultipartManager<ServerSideMultipartUpload, MantaMultipartUploadPart> {
    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerSideMultipartManager.class);

    /**
     * Maximum number of parts to allow in a multipart upload.
     */
    private static final int MAX_PARTS = 10_000;

    /**
     * Minimum size of a part in bytes.
     */
    private static final int MIN_PART_SIZE = 5_242_880; // 5 mebibytes

    /**
     * Configuration context used to get home directory.
     */
    private final ConfigContext config;

    /**
     * Reference to the Apache HTTP Client HTTP request creation class.
     */
    private final MantaConnectionFactory connectionFactory;

    /**
     * Current connection context used for maintaining state between requests.
     */
    private final MantaConnectionContext connectionContext;

    /**
     * Reference to an open client.
     */
    private final MantaClient mantaClient;

    /**
     * Creates a new instance of a server-side MPU manager using the specified
     * configuration and connection builder objects.
     *
     * @param config configuration context
     * @param connectionFactory connection configuration and setup object
     * @param connectionContext connection execution object
     */
    public ServerSideMultipartManager(final ConfigContext config,
                                      final MantaConnectionFactory connectionFactory,
                                      final MantaConnectionContext connectionContext,
                                      final MantaClient mantaClient) {
        Validate.isTrue(!mantaClient.isClosed(), "MantaClient must not be closed");

        this.config = config;
        this.connectionFactory = connectionFactory;
        this.connectionContext = connectionContext;
        this.mantaClient = mantaClient;
    }

    @Override
    public Stream<ServerSideMultipartUpload> listInProgress() throws IOException {
        final String uploadsPath = uploadsPath();

        return mantaClient.listObjects(uploadsPath).map(mantaObject -> {
            final String objectName = FilenameUtils.getName(mantaObject.getPath());
            final UUID id = UUID.fromString(objectName);

            // We don't know the final object name. The server will implement
            // this as a feature in the future.

            return new ServerSideMultipartUpload(id, null, uuidPrefixedPath(id));
        });
    }

    @Override
    public ServerSideMultipartUpload initiateUpload(final String path) throws IOException {
        return initiateUpload(path, null, null);
    }

    @Override
    public ServerSideMultipartUpload initiateUpload(final String path,
                                                    final MantaMetadata mantaMetadata)
            throws IOException {
        return initiateUpload(path, mantaMetadata, null);
    }

    @Override
    public ServerSideMultipartUpload initiateUpload(final String path,
                                                    final MantaMetadata mantaMetadata,
                                                    final MantaHttpHeaders httpHeaders)
            throws IOException {
        final String postPath = uploadsPath();
        final HttpPost post = connectionFactory.post(postPath);

        final byte[] jsonRequest = createMpuRequestBody(path, mantaMetadata, httpHeaders);
        final HttpEntity entity = new ExposedByteArrayEntity(
                jsonRequest, ContentType.APPLICATION_JSON);
        post.setEntity(entity);

        final int expectedStatusCode = HttpStatus.SC_CREATED;

        try (CloseableHttpResponse response = connectionContext.getHttpClient().execute(post)) {
            StatusLine statusLine = response.getStatusLine();

            validateStatusCode(expectedStatusCode, statusLine.getStatusCode(),
                    "Unable to create multipart upload", post,
                    response, path, jsonRequest);
            validateEntityIsPresent(post, response, path, jsonRequest);

            try (InputStream in = response.getEntity().getContent()) {
                ObjectNode mpu = MantaObjectMapper.INSTANCE.readValue(in, ObjectNode.class);

                JsonNode idNode = mpu.get("id");
                Validate.notNull(idNode, "No multipart id returned in response");
                UUID uploadId = UUID.fromString(idNode.textValue());

                JsonNode partsDirectoryNode = mpu.get("partsDirectory");
                Validate.notNull(partsDirectoryNode, "No parts directory returned in response");
                String partsDirectory = partsDirectoryNode.textValue();

                return new ServerSideMultipartUpload(uploadId, path, partsDirectory);
            } catch (NullPointerException | IllegalArgumentException e) {
                String msg = "Expected response field was missing or malformed";
                MantaMultipartException me = new MantaMultipartException(msg, e);
                annotateException(me, post, response, path, jsonRequest);
                throw me;
            } catch (JsonParseException e) {
                String msg = "Response body was not JSON";
                MantaMultipartException me = new MantaMultipartException(msg, e);
                annotateException(me, post, response, path, jsonRequest);
                throw me;
            }
        }
    }

    @Override
    public MantaMultipartUploadPart uploadPart(final ServerSideMultipartUpload upload,
                                               final int partNumber,
                                               final String contents)
            throws IOException {
        Validate.inclusiveBetween(partNumber, 1, MAX_PARTS,
                "Part numbers must be inclusively between [1-{}]", MAX_PARTS);
        Validate.notNull(contents, "String must not be null");

        HttpEntity entity = new ExposedStringEntity(contents, ContentType.APPLICATION_OCTET_STREAM);

        if (entity.getContentLength() < MIN_PART_SIZE) {
            String msg = String.format("Part size [%d] for string is less "
                            + "that the minimum part size [%d]",
                    entity.getContentLength(), MIN_PART_SIZE);
            throw new IllegalArgumentException(msg);
        }

        return uploadPart(upload, partNumber, entity);
    }

    @Override
    public MantaMultipartUploadPart uploadPart(final ServerSideMultipartUpload upload,
                                               final int partNumber,
                                               final byte[] bytes)
            throws IOException {
        Validate.inclusiveBetween(partNumber, 1, MAX_PARTS,
                "Part numbers must be inclusively between [1-{}]", MAX_PARTS);
        Validate.notNull(bytes, "Byte array must not be null");

        if (bytes.length < MIN_PART_SIZE) {
            String msg = String.format("Part size [%d] for byte array is less "
                            + "that the minimum part size [%d]",
                    bytes.length, MIN_PART_SIZE);
            throw new IllegalArgumentException(msg);
        }

        HttpEntity entity = new ExposedByteArrayEntity(bytes, ContentType.APPLICATION_OCTET_STREAM);
        return uploadPart(upload, partNumber, entity);
    }

    @Override
    public MantaMultipartUploadPart uploadPart(final ServerSideMultipartUpload upload,
                                               final int partNumber,
                                               final File file) throws IOException {
        Validate.inclusiveBetween(partNumber, 1, MAX_PARTS,
                "Part numbers must be inclusively between [1-{}]", MAX_PARTS);
        Validate.notNull(file, "File must not be null");

        if (file.length() < MIN_PART_SIZE) {
            String msg = String.format("Part size [%d] for file [%s] is less "
                    + "that the minimum part size [%d]",
                    file.length(), file.getPath(), MIN_PART_SIZE);
            throw new IllegalArgumentException(msg);
        }

        HttpEntity entity = new FileEntity(file, ContentType.APPLICATION_OCTET_STREAM);
        return uploadPart(upload, partNumber, entity);
    }

    @Override
    public MantaMultipartUploadPart uploadPart(final ServerSideMultipartUpload upload,
                                               final int partNumber,
                                               final InputStream inputStream)
            throws IOException {
        HttpEntity entity = new MantaInputStreamEntity(inputStream, ContentType.APPLICATION_OCTET_STREAM);
        return uploadPart(upload, partNumber, entity);
    }

    private MantaMultipartUploadPart uploadPart(final ServerSideMultipartUpload upload,
                                                final int partNumber,
                                                final HttpEntity entity)
            throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");
        Validate.inclusiveBetween(partNumber, 1, MAX_PARTS,
                "Part numbers must be inclusively between [1-{}]", MAX_PARTS);

        final String putPath = upload.getPartsDirectory() + SEPARATOR + partNumber;
        final HttpPut put = connectionFactory.put(putPath);
        put.setEntity(entity);

        try (CloseableHttpResponse response = connectionContext.getHttpClient().execute(put)) {
            Header etagHeader = response.getFirstHeader(HttpHeaders.ETAG);
            final String etag;

            if (etagHeader != null) {
                etag = etagHeader.getValue();
            } else {
                etag = null;
            }

            return new MantaMultipartUploadPart(partNumber, upload.getPath(), etag);
        }
    }

    @Override
    public MantaMultipartUploadPart getPart(final ServerSideMultipartUpload upload,
                                            final int partNumber) throws IOException {

        Validate.notNull(upload, "Upload state object must not be null");

        final String getPath = upload.getPartsDirectory() + SEPARATOR + "state";
        final HttpGet get = connectionFactory.get(getPath);

        final String objectPath;

        final int expectedStatusCode = HttpStatus.SC_OK;

        try (CloseableHttpResponse response = connectionContext.getHttpClient().execute(get)) {
            StatusLine statusLine = response.getStatusLine();
            validateStatusCode(expectedStatusCode, statusLine.getStatusCode(),
                    "Unable to get status for multipart upload", get,
                    response, null, null);
            validateEntityIsPresent(get, response, null, null);

            try (InputStream in = response.getEntity().getContent()) {
                ObjectNode objectNode = MantaObjectMapper.INSTANCE.readValue(in, ObjectNode.class);

                JsonNode objectPathNode = objectNode.get("objectPath");
                Validate.notNull(objectPathNode, "Unable to read object path from response");
                objectPath = objectPathNode.textValue();
                Validate.notBlank(objectPath, "Object path field was blank in response");
            } catch (JsonParseException e) {
                String msg = "Response body was not JSON";
                MantaMultipartException me = new MantaMultipartException(msg, e);
                annotateException(me, get, response, null, null);
                throw me;
            } catch (NullPointerException | IllegalArgumentException e) {
                String msg = "Expected response field was missing or malformed";
                MantaMultipartException me = new MantaMultipartException(msg, e);
                annotateException(me, get, response, null, null);
                throw me;
            }
        }

        final String headPath = upload.getPartsDirectory() + SEPARATOR + partNumber;
        final HttpHead head = connectionFactory.head(headPath);

        final String etag;

        try (CloseableHttpResponse response = connectionContext.getHttpClient().execute(head)) {
            StatusLine statusLine = response.getStatusLine();

            if (statusLine.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return null;
            }

            validateStatusCode(expectedStatusCode, statusLine.getStatusCode(),
                    "Unable to get status for multipart upload part", get,
                    response, null, null);

            try {
                final Header etagHeader = response.getFirstHeader(HttpHeaders.ETAG);
                Validate.notNull(etagHeader, "ETag header was not returned");
                etag = etagHeader.getValue();
                Validate.notBlank(etag, "ETag is blank");
            } catch (NullPointerException | IllegalArgumentException e) {
                String msg = "Expected header was missing or malformed";
                MantaMultipartException me = new MantaMultipartException(msg, e);
                annotateException(me, get, response, null, null);
                throw me;
            }
        }

        return new MantaMultipartUploadPart(partNumber, objectPath, etag);
    }

    @Override
    public MantaMultipartStatus getStatus(final ServerSideMultipartUpload upload)
            throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");

        final String getPath = upload.getPartsDirectory() + SEPARATOR + "state";
        final HttpGet get = connectionFactory.get(getPath);

        final int expectedStatusCode = HttpStatus.SC_OK;

        try (CloseableHttpResponse response = connectionContext.getHttpClient().execute(get)) {
            StatusLine statusLine = response.getStatusLine();
            validateStatusCode(expectedStatusCode, statusLine.getStatusCode(),
                    "Unable to get status for multipart upload", get,
                    response, null, null);
            validateEntityIsPresent(get, response, null, null);

            try (InputStream in = response.getEntity().getContent()) {
                ObjectNode objectNode = MantaObjectMapper.INSTANCE.readValue(in, ObjectNode.class);

                JsonNode stateNode = objectNode.get("state");
                Validate.notNull(stateNode, "Unable to get state from response");
                String state = stateNode.textValue();
                Validate.notBlank(state, "State field was blank in response");

                if (state.equals("CREATED")) {
                    return MantaMultipartStatus.CREATED;
                }

                if (state.equals("FINALIZING")) {
                    JsonNode typeNode = objectNode.get("type");
                    Validate.notNull(typeNode, "Unable to get type from response");
                    String type = typeNode.textValue();
                    Validate.notBlank(type, "Type field was blank in response");

                    if (type.equals("COMMIT")) {
                        return MantaMultipartStatus.COMMITTING;
                    }
                    if (type.equals("ABORT")) {
                        return MantaMultipartStatus.ABORTING;
                    }
                }

                return MantaMultipartStatus.UNKNOWN;
            } catch (JsonParseException e) {
                String msg = "Response body was not JSON";
                MantaMultipartException me = new MantaMultipartException(msg, e);
                annotateException(me, get, response, null, null);
                throw me;
            }  catch (NullPointerException | IllegalArgumentException e) {
                String msg = "Expected response field was missing or malformed";
                MantaMultipartException me = new MantaMultipartException(msg, e);
                annotateException(me, get, response, null, null);
                throw me;
            }
        }
    }

    @Override
    public Stream<MantaMultipartUploadPart> listParts(final ServerSideMultipartUpload upload)
            throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");
        return null;
    }

    @Override
    public void validateThatThereAreSequentialPartNumbers(final ServerSideMultipartUpload upload)
            throws IOException, MantaMultipartException {
        Validate.notNull(upload, "Upload state object must not be null");
    }

    @Override
    public void abort(final ServerSideMultipartUpload upload) throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");

        final String postPath = upload.getPartsDirectory() + SEPARATOR + "abort";
        final HttpPost post = connectionFactory.post(postPath);

        final int expectedStatusCode = HttpStatus.SC_NO_CONTENT;

        try (CloseableHttpResponse response = connectionContext.getHttpClient().execute(post)) {
            StatusLine statusLine = response.getStatusLine();
            validateStatusCode(expectedStatusCode, statusLine.getStatusCode(),
                    "Unable to abort multipart upload", post,
                    response, null, null);
            LOGGER.info("Aborted multipart upload [id={}]", upload.getId());
        }
    }

    @Override
    public void complete(final ServerSideMultipartUpload upload,
                         final Iterable<? extends MantaMultipartUploadTuple> parts)
            throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");
    }

    @Override
    public void complete(final ServerSideMultipartUpload upload,
                         final Stream<? extends MantaMultipartUploadTuple> partsStream)
            throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");
    }

    @Override
    public <R> R waitForCompletion(final ServerSideMultipartUpload upload,
                                   final Function<UUID, R> executeWhenTimesToPollExceeded)
            throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");
        return null;
    }

    @Override
    public <R> R waitForCompletion(final ServerSideMultipartUpload upload,
                                   final Duration pingInterval,
                                   final int timesToPoll,
                                   final Function<UUID, R> executeWhenTimesToPollExceeded)
            throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");
        return null;
    }

    /**
     * Creates the JSON request body used to create a new multipart upload request.
     *
     * @param objectPath path to the object on Manta
     * @param mantaMetadata metadata associated with object
     * @param headers HTTP headers associated with object
     *
     * @return byte array containing JSON data
     */
    byte[] createMpuRequestBody(final String objectPath,
                                final MantaMetadata mantaMetadata,
                                final MantaHttpHeaders headers) {
        Validate.notNull(objectPath, "Path to Manta object must not be null");

        CreateMPURequestBody requestBody = new CreateMPURequestBody(
                objectPath, mantaMetadata, headers);

        try {
            return MantaObjectMapper.INSTANCE.writeValueAsBytes(requestBody);
        } catch (IOException e) {
            String msg = "Error serializing JSON for MPU request body";
            throw new MantaMultipartException(msg, e);
        }
    }

    /**
     * Validates that the status code received is the expected status code.
     *
     * @param expectedCode expected HTTP status code
     * @param actualCode actual HTTP status code
     * @param errorMessage error message to attach to exception
     * @param request HTTP request object
     * @param response HTTP response object
     * @param objectPath path to the object being operated on
     * @param requestBody contents of request body as byte array
     * @throws MantaMultipartException thrown when the status codes do not match
     */
    private void validateStatusCode(final int expectedCode,
                                    final int actualCode,
                                    final String errorMessage,
                                    final HttpRequest request,
                                    final HttpResponse response,
                                    final String objectPath,
                                    final byte[] requestBody) {
        if (actualCode != expectedCode) {
            MantaMultipartException e = new MantaMultipartException(errorMessage);
            annotateException(e, request, response, objectPath, requestBody);
            throw e;
        }
    }

    /**
     * @return path to the server-side multipart uploads directory
     */
    private String uploadsPath() {
        return config.getMantaHomeDirectory() + SEPARATOR + "uploads";
    }

    /**
     * Creates a <code>$home/uploads/directory/directory</code> path with
     * the first letter of a uuid being the first directory and the uuid itself
     * being the second directory.
     *
     * @param uuid uuid to create path from
     * @return uuid prefixed directories
     */
    String uuidPrefixedPath(final UUID uuid) {
        Validate.notNull(uuid, "UUID must not be null");

        final String uuidString = uuid.toString();

        return uploadsPath() + SEPARATOR
                + uuidString.substring(0, 1)
                + SEPARATOR + uuidString;
    }

    /**
     * Validates that the response has a valid entity.
     *
     * @param request HTTP request object
     * @param response HTTP response object
     * @param objectPath path to the object being operated on
     * @param requestBody contents of request body as byte array
     * @throws MantaMultipartException thrown when the entity is null
     * @throws MantaIOException thrown when unable to get entity's InputStream
     */
    private void validateEntityIsPresent(final HttpRequest request,
                                         final HttpResponse response,
                                         final String objectPath,
                                         final byte[] requestBody)
            throws MantaIOException {
        if (response.getEntity() == null) {
            String msg = "Entity response was null";
            MantaMultipartException e = new MantaMultipartException(msg);
            annotateException(e, request, response, objectPath, requestBody);
            throw e;
        }

        try {
            if (response.getEntity().getContent() == null) {
                String msg = "Entity content InputStream was null";
                MantaMultipartException e = new MantaMultipartException(msg);
                annotateException(e, request, response, objectPath, requestBody);
                throw e;
            }
        } catch (IOException e) {
            String msg = "Unable to get an InputStream from the HTTP entity";
            MantaIOException mioe = new MantaIOException(msg, e);
            annotateException(mioe, request, response, objectPath, requestBody);
            throw mioe;
        }
    }

    /**
     * Appends context attributes for the HTTP request and HTTP response objects
     * to a {@link ExceptionContext} instance using values relevant to this
     * class.
     *
     * @param exception exception to append to
     * @param request HTTP request object
     * @param response HTTP response object
     * @param objectPath path to the object being operated on
     * @param requestBody contents of request body as byte array
     */
    private void annotateException(final ExceptionContext exception,
                                   final HttpRequest request,
                                   final HttpResponse response,
                                   final String objectPath,
                                   final byte[] requestBody) {
        HttpHelper.annotateContextedException(exception, request, response);

        if (objectPath != null) {
            exception.setContextValue("objectPath", objectPath);
        }

        if (requestBody != null) {
            exception.setContextValue("requestBody", new String(requestBody, Charsets.UTF_8));
        }
    }
}