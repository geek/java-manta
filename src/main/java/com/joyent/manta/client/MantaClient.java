/**
 * Copyright (c) 2014, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import java.io.*;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.EmptyContent;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.ObjectParser;
import com.joyent.manta.client.crypto.HttpSigner;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaCryptoException;
import com.joyent.manta.exception.MantaObjectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manta Http client.
 *
 * @author Yunong Xiao
 */
public class MantaClient {
    private static final Logger LOG = LoggerFactory.getLogger(MantaClient.class);

    private static final JsonFactory JSON_FACTORY = new JacksonFactory();
    private static final HttpRequestFactory HTTP_REQUEST_FACTORY = new NetHttpTransport()
    .createRequestFactory(new HttpRequestInitializer() {

        @Override
        public void initialize(final HttpRequest request) throws IOException {
            request.setParser(new JsonObjectParser(JSON_FACTORY));
        }
    });

    private static final String LINK_CONTENT_TYPE = "application/json; type=link";
    private static final String DIRECTORY_CONTENT_TYPE = "application/json; type=directory";

    /**
     * Creates a new instance of a Manta client.
     * @param config
     *               The configuration context that provides all of the configuration values.
     * @return An instance of {@link MantaClient}
     * @throws IOException
     *             If unable to instantiate the client.
     */
    public static MantaClient newInstance(final ConfigContext config) throws IOException {
        return newInstance(config.getMantaURL(),
                           config.getMantaUser(),
                           config.getMantaKeyPath(),
                           config.getMantaKeyId());
    }

    /**
     * Creates a new instance of a Manta client.
     *
     * @param url
     *            The url of the Manta endpoint.
     * @param login
     *            The user login name.
     * @param keyPath
     *            The path to the user rsa private key on disk.
     * @param fingerPrint
     *            The fingerprint of the user rsa private key.
     * @return An instance of {@link MantaClient}
     * @throws IOException
     *             If unable to instantiate the client.
     */
    public static MantaClient newInstance(final String url, final String login, final String keyPath,
                                          final String fingerPrint) throws IOException {
        return newInstance(url, login, keyPath, fingerPrint, DefaultsConfigContext.DEFAULT_HTTP_TIMEOUT);
    }

    /**
     * Creates a new instance of a Manta client.
     *
     * @param url
     *            The url of the Manta endpoint.
     * @param login
     *            The user login name.
     * @param keyPath
     *            The path to the user rsa private key on disk.
     * @param fingerPrint
     *            The fingerprint of the user rsa private key.
     * @param timeout
     *            The http timeout in milliseconds.
     * @return An instance of {@link MantaClient}
     * @throws IOException
     *             If unable to instantiate the client.
     */
    public static MantaClient newInstance(final String url, final String login, final String keyPath,
                                          final String fingerPrint, final int timeout) throws IOException {
        LOG.debug(String.format("entering newInstance with url %s, login %s, keyPath %s, fingerPrint %s, timeout %s",
                                url, login, keyPath, fingerPrint, timeout));
        return new MantaClient(url, login, keyPath, fingerPrint, timeout);
    }

    /**
     * Creates a new instance of a Manta client.
     *
     * @param url
     *            The url of the Manta endpoint.
     * @param login
     *            The user login name.
     * @param privateKeyContent
     *            The user's rsa private key as a string.
     * @param fingerPrint
     *            The fingerprint of the user rsa private key.
     * @param password
     *            The private key password (optional).
     * @return An instance of {@link MantaClient}
     * @throws IOException
     *             If unable to instantiate the client.
     */
    public static MantaClient newInstance(final String url, final String login, final String privateKeyContent,
                                          final String fingerPrint, final char[] password) throws IOException {
        return newInstance(url, login, privateKeyContent, fingerPrint, password, DefaultsConfigContext.DEFAULT_HTTP_TIMEOUT);
    }

    /**
     * Creates a new instance of a Manta client.
     *
     * @param url
     *            The url of the Manta endpoint.
     * @param login
     *            The user login name.
     * @param privateKeyContent
     *            The private key as a string.
     * @param fingerPrint
     *            The fingerprint of the user rsa private key.
     * @param password
     *            The private key password (optional).
     * @param timeout
     *            The HTTP timeout in milliseconds.
     * @return An instance of {@link MantaClient}
     * @throws IOException
     *             If unable to instantiate the client.
     */
    public static MantaClient newInstance(final String url, final String login, final String privateKeyContent,
                                          final String fingerPrint, final char[] password, final int timeout)
                                                  throws IOException {
        LOG.debug(String
                  .format("entering newInstance with url %s, login %s, privateKey ?, fingerPrint %s, password ?, "
                          + "timeout %d", url, login, fingerPrint, timeout));
        return new MantaClient(url, login, privateKeyContent, fingerPrint, password, timeout);
    }

    private final String url_;

    private final HttpSigner httpSigner_;

    private final int httpTimeout_;

    private MantaClient(final String url, final String login, final String keyPath, final String fingerPrint,
                        final int httpTimeout) throws IOException {
        this.url_ = url;
        this.httpSigner_ = HttpSigner.newInstance(keyPath, fingerPrint, login);
        this.httpTimeout_ = httpTimeout;
    }

    private MantaClient(final String url, final String login, final String privateKeyContent, final String keyName,
                        final char[] password, final int httpTimeout) throws IOException {
        this.url_ = url;
        this.httpSigner_ = HttpSigner.newInstance(privateKeyContent, keyName, password, login);
        this.httpTimeout_ = httpTimeout;
    }

    /**
     * Deletes an object from Manta.
     *
     * @param path
     *            The fully qualified path of the Manta object.
     * @throws IOException
     *             If an IO exception has occured.
     * @throws MantaCryptoException
     *             If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException
     *             If an HTTP status code > 300 is returned.
     */
    public void delete(final String path) throws MantaCryptoException, MantaClientHttpResponseException, IOException {
        LOG.debug(String.format("entering delete with path %s", path));
        final GenericUrl url = new GenericUrl(this.url_ + formatPath(path));
        final HttpRequest request = HTTP_REQUEST_FACTORY.buildDeleteRequest(url);
        // XXX: make an intercepter that sets these timeouts before each API call.
        request.setReadTimeout(this.httpTimeout_);
        request.setConnectTimeout(this.httpTimeout_);
        this.httpSigner_.signRequest(request);
        HttpResponse response = null;
        try {
            response = request.execute();
            LOG.debug(String.format("got response code %s, header %s ",
                                    response.getStatusCode(), response.getHeaders()));
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }
    }

    /**
     * Recursively deletes an object in Manta.
     *
     * @param path
     *            The fully qualified path of the Manta object.
     * @throws IOException
     *             If an IO exception has occured.
     * @throws MantaCryptoException
     *             If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException
     *             If a http status code > 300 is returned.
     */
    public void deleteRecursive(final String path) throws MantaCryptoException, MantaClientHttpResponseException,
    IOException {
        LOG.debug(String.format("entering deleteRecursive with path %s", path));
        Collection<MantaObject> objs;
        try {
            objs = this.listObjects(path);
        } catch (final MantaObjectException e) {
            this.delete(path);
            LOG.debug(String.format("finished deleting path %s", path));
            return;
        }
        for (final MantaObject mantaObject : objs) {
            if (mantaObject.isDirectory()) {
                this.deleteRecursive(mantaObject.getPath());
            } else {
                this.delete(mantaObject.getPath());
            }
        }

        try {
            this.delete(path);
        } catch (MantaClientHttpResponseException e) {
            if (e.getStatusCode() == 404 && LOG.isDebugEnabled()) {
                LOG.debug("Couldn't delete object because it doesn't exist", e);
            } else {
                throw e;
            }
        }

        LOG.debug(String.format("finished deleting path %s", path));
    }

    /**
     * Get a Manta object.
     *
     * @param path
     *            The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return The {@link MantaObject}.
     * @throws IOException
     *             If an IO exception has occured.
     * @throws MantaCryptoException
     *             If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException
     *             If a http status code > 300 is returned.
     */
    public MantaObject get(final String path) throws MantaCryptoException, MantaClientHttpResponseException,
    IOException {
        LOG.debug(String.format("entering get with path %s", path));
        final GenericUrl url = new GenericUrl(this.url_ + formatPath(path));
        final HttpRequest request = HTTP_REQUEST_FACTORY.buildGetRequest(url);
        request.setReadTimeout(this.httpTimeout_);
        request.setConnectTimeout(this.httpTimeout_);
        this.httpSigner_.signRequest(request);
        HttpResponse response;
        try {
            response = request.execute();
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        }
        final MantaObject mantaObject = new MantaObject(path, response.getHeaders());
        mantaObject.setDataInputStream(response.getContent());
        mantaObject.setHttpHeaders(response.getHeaders());
        LOG.debug(String.format("got response code %s, MantaObject %s, header %s ", response.getStatusCode(),
                                mantaObject, response.getHeaders()));
        return mantaObject;
    }

    /**
     * Get the metadata associated with a Manta object.
     *
     * @param path
     *            The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return The {@link MantaObject}.
     * @throws IOException
     *             If an IO exception has occurred.
     * @throws MantaCryptoException
     *             If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException
     *             If a http status code > 300 is returned.
     */
    public MantaObject head(final String path) throws MantaCryptoException, MantaClientHttpResponseException,
    IOException {
        LOG.debug(String.format("entering get with path %s", path));
        final GenericUrl url = new GenericUrl(this.url_ + formatPath(path));
        final HttpRequest request = HTTP_REQUEST_FACTORY.buildHeadRequest(url);
        request.setReadTimeout(this.httpTimeout_);
        request.setConnectTimeout(this.httpTimeout_);
        this.httpSigner_.signRequest(request);
        HttpResponse response;
        try {
            response = request.execute();
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        }
        final MantaObject mantaObject = new MantaObject(path, response.getHeaders());
        LOG.debug(String.format("got response code %s, MantaObject %s, header %s", response.getStatusCode(),
                                mantaObject, response.getHeaders()));
        return mantaObject;
    }

    /**
     * Return the contents of a directory in Manta.
     *
     * @param path
     *            The fully qualified path of the directory.
     * @return A {@link Collection} of {@link MantaObject} listing the contents of the directory.
     * @throws IOException
     *             If an IO exception has occured.
     * @throws MantaCryptoException
     *             If there's an exception while signing the request.
     * @throws MantaObjectException
     *             If the path isn't a directory
     * @throws MantaClientHttpResponseException
     *             If a http status code > 300 is returned.
     */
    public Collection<MantaObject> listObjects(final String path) throws MantaCryptoException, MantaObjectException,
            MantaClientHttpResponseException, IOException {
        LOG.debug(String.format("entering listDirectory with directory %s", path));
        final GenericUrl url = new GenericUrl(this.url_ + formatPath(path));
        final HttpRequest request = HTTP_REQUEST_FACTORY.buildGetRequest(url);
        request.setReadTimeout(this.httpTimeout_);
        request.setConnectTimeout(this.httpTimeout_);
        this.httpSigner_.signRequest(request);
        HttpResponse response = null;
        try {
            response = request.execute();
            LOG.debug(String.format("got response code %s, header %s ", response.getStatusCode(),
                                    response.getHeaders()));

            if (!response.getContentType().equals(MantaObject.DIRECTORY_HEADER)) {
                throw new MantaObjectException("Object is not a directory");
            }

            final BufferedReader br = new BufferedReader(new InputStreamReader(response.getContent()));
            final ObjectParser parser = request.getParser();
            return buildObjects(path, br, parser);
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }
    }

    /**
     * Creates a list of {@link MantaObject}s based on the HTTP response from Manta.
     * @param path
     *            The fully qualified path of the directory.
     * @param content
     *            The content of the response as a Reader.
     * @param parser
     *            Deserializer implementation that takes the raw content and turns it into a {@link MantaObject}
     * @return List of {@link MantaObject}s for a given directory
     * @throws IOException
     *            If an IO exception has occurred.
     */
    protected static List<MantaObject> buildObjects(String path, BufferedReader content, ObjectParser parser) throws IOException {
        final ArrayList<MantaObject> objs = new ArrayList<>();
        String line;
        StringBuilder myPath = new StringBuilder(path);
        while ((line = content.readLine()) != null) {
            final MantaObject obj = parser.parseAndClose(new StringReader(line), MantaObject.class);
            // need to prefix the obj name with the fully qualified path, since Manta only returns
            // the explicit name of the object.
            if (!MantaUtils.endsWith(myPath, '/')) {
                myPath.append('/');
            }

            obj.setPath(myPath + obj.getPath());
            objs.add(obj);
        }
        return objs;
    }


    /**
     * Puts an object into Manta.
     *
     * @param object
     *            The stored Manta object. This must contain the fully qualified path of the object, along with optional
     *            data either stored as an {@link InputStream}, {@link File}, or {@link String}.
     * @throws IOException
     *             If an IO exception has occured.
     * @throws MantaCryptoException
     *             If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException
     *             If a http status code > 300 is returned.
     */
    public void put(final MantaObject object) throws MantaCryptoException, MantaClientHttpResponseException,
    IOException {
        LOG.debug(String.format("entering put with manta object %s, headers %s", object, object.getHttpHeaders()));
        String contentType = null;
        if (object.getHttpHeaders() != null) {
            contentType = object.getHttpHeaders().getContentType();
        }
        HttpContent content;
        if (object.getDataInputStream() != null) {
            content = new InputStreamContent(contentType, object.getDataInputStream());
        } else if (object.getDataInputFile() != null) {
            content = new FileContent(contentType, object.getDataInputFile());
        } else if (object.getDataInputString() != null) {
            content = new ByteArrayContent(contentType, object.getDataInputString().getBytes("UTF-8"));
        } else {
            content = new EmptyContent();
        }
        final GenericUrl url = new GenericUrl(this.url_ + formatPath(object.getPath()));

        final HttpRequest request = HTTP_REQUEST_FACTORY.buildPutRequest(url, content);
        request.setReadTimeout(this.httpTimeout_);
        request.setConnectTimeout(this.httpTimeout_);
        if (object.getHttpHeaders() != null) {
            request.setHeaders(object.getHttpHeaders());
        }
        this.httpSigner_.signRequest(request);
        HttpResponse response = null;
        try {
            LOG.debug(String.format("sending request %s", request));
            response = request.execute();
            LOG.debug(String.format("got response code %s, header %s ", response.getStatusCode(),
                                    response.getHeaders()));
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }
    }

    /**
     * Creates a directory in Manta.
     *
     * @param path
     *            The fully qualified path of the Manta directory.
     * @param headers
     *            Optional {@link HttpHeaders}. Consult the Manta api for more header information.
     * @throws IOException
     *             If an IO exception has occured.
     * @throws MantaCryptoException
     *             If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException
     *             If a http status code > 300 is returned.
     */
    public void putDirectory(final String path, final HttpHeaders headers)
            throws MantaCryptoException, MantaClientHttpResponseException, IOException {
        if (path == null) {
            throw new IllegalArgumentException("PUT directory path can't be null");
        }

        LOG.debug(String.format("entering putDirectory with directory %s", path));
        final GenericUrl url = new GenericUrl(this.url_ + formatPath(path));
        final HttpRequest request = HTTP_REQUEST_FACTORY.buildPutRequest(url, new EmptyContent());
        request.setReadTimeout(this.httpTimeout_);
        request.setConnectTimeout(this.httpTimeout_);
        if (headers != null) {
            request.setHeaders(headers);
        }
        this.httpSigner_.signRequest(request);
        request.getHeaders().setContentType(DIRECTORY_CONTENT_TYPE);
        HttpResponse response = null;
        try {
            response = request.execute();
            LOG.debug(String.format("got response code %s, header %s ", response.getStatusCode(),
                                    response.getHeaders()));
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }
    }

    /**
     * Create a Manta snaplink.
     *
     * @param linkPath
     *            The fully qualified path of the new snaplink.
     * @param objectPath
     *            The fully qualified path of the object to link against.
     * @param headers
     *            Optional {@link HttpHeaders}. Consult the Manta api for more header information.
     * @throws IOException
     *             If an IO exception has occured.
     * @throws MantaCryptoException
     *             If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException
     *             If a http status code > 300 is returned.
     */
    public void putSnapLink(final String linkPath, final String objectPath, final HttpHeaders headers)
            throws MantaCryptoException, MantaClientHttpResponseException, IOException {
        LOG.debug(String.format("entering putLink with link %s, path %s", linkPath, objectPath));
        final GenericUrl url = new GenericUrl(this.url_ + formatPath(linkPath));
        final HttpContent content = new EmptyContent();
        final HttpRequest request = HTTP_REQUEST_FACTORY.buildPutRequest(url, content);
        if (headers != null) {
            request.setHeaders(headers);
        }
        this.httpSigner_.signRequest(request);
        request.getHeaders().setContentType(LINK_CONTENT_TYPE);
        request.getHeaders().setLocation(formatPath(objectPath));
        HttpResponse response = null;
        try {
            response = request.execute();
            LOG.debug(String.format("got response code %s, header %s ", response.getStatusCode(),
                                    response.getHeaders()));
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }
    }

    /**
     * Format the path according to RFC3986.
     *
     * @param path
     *            the raw path string.
     * @return the URI formatted string with the exception of '/' which is special in manta.
     * @throws UnsupportedEncodingException
     *             If UTF-8 is not supported on this system.
     */
    private static String formatPath(final String path) throws UnsupportedEncodingException {
        // first split the path by slashes.
        final String[] elements = path.split("/");
        final StringBuilder encodedPath = new StringBuilder();
        for (final String string : elements) {
            if (string.equals("")) {
                continue;
            }
            encodedPath.append("/").append(URLEncoder.encode(string, "UTF-8"));
        }
        return encodedPath.toString();
    }
}
