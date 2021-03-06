package com.joyent.manta.client;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.joyent.manta.exception.MantaClientException;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A read-only {@link SeekableByteChannel} implementation that utilizes
 * the HTTP Range header to allow you to seek any position in an object on
 * Manta. Connection opening to the remote server happens lazily upon the
 * first read() or size() method invoked.
 *
 * @author Elijah Zupancic
 */
@ThreadSafe
public class MantaSeekableByteChannel extends InputStream
        implements SeekableByteChannel {
    /**
     * Constant representing the value returned when we have reached the
     * end of a stream.
     */
    private static final int EOF = -1;

    /**
     * Flag indicating if the channel is open. Marked as volatile so
     * that different threads can flip its state.
     */
    private volatile boolean open = true;

    /**
     * URL of the object on the Manta API.
     */
    private final GenericUrl objectUri;

    /**
     * Current position in bytes from the start of the file.
     */
    private volatile long position = 0L;

    /**
     * The provider for http requests setup, metadata and request initialization.
     */
    private final HttpRequestFactory httpRequestFactory;

    /**
     * Threadsafe reference to the Manta API HTTP response object.
     */
    private final AtomicReference<HttpResponse> responseRef;

    /**
     * Creates a new instance of a read-only seekable byte channel.
     *
     * @param objectUri URL of the object on the Manta API
     * @param position starting position in bytes from the start of the file
     * @param httpRequestFactory provider for http requests setup, metadata and request initialization
     */
    public MantaSeekableByteChannel(final GenericUrl objectUri,
                                    final long position,
                                    final HttpRequestFactory httpRequestFactory) {
        this.objectUri = objectUri;
        this.position = position;
        this.httpRequestFactory = httpRequestFactory;
        this.responseRef = new AtomicReference<>();

    }


    /**
     * Creates a new instance of a read-only seekable byte channel.
     *
     * @param objectUri URL of the object on the Manta API
     * @param httpRequestFactory provider for http requests setup, metadata and request initialization
     */
    public MantaSeekableByteChannel(final GenericUrl objectUri,
                                    final HttpRequestFactory httpRequestFactory) {
        this(objectUri, 0L, httpRequestFactory);
    }

    /**
     * Constructor used for creating a new instance of a read-only seekable byte
     * channel from within this class. This is used when position() is called.
     *
     * @param responseRef reference to existing HTTP response
     * @param objectUri URL of the object on the Manta API
     * @param position starting position in bytes from the start of the file
     * @param httpRequestFactory provider for http requests setup, metadata and request initialization
     */
    protected MantaSeekableByteChannel(final AtomicReference<HttpResponse> responseRef,
                                       final GenericUrl objectUri,
                                       final long position,
                                       final HttpRequestFactory httpRequestFactory) {
        this.responseRef = responseRef;
        this.objectUri = objectUri;
        this.position = position;
        this.httpRequestFactory = httpRequestFactory;
    }

    @Override
    public int read(final ByteBuffer dst) throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final HttpResponse response = connectOrGetResponse();
        final long size = size();

        if (position >= size) {
            return EOF;
        }

        final InputStream is = response.getContent();
        final byte[] buff = dst.array();
        final int bytesRead = is.read(buff);

        position += bytesRead;

        return bytesRead;
    }

    /**
     * Reads the next byte of data from the backing input stream at the current
     * position. The value byte is returned as an <code>int</code> in the range
     * <code>0</code> to <code>255</code>. If no byte is available because the
     * end of the stream has been reached, the value <code>-1</code> is returned.
     * This method blocks until input data is available, the end of the stream
     * is detected, or an exception is thrown.
     *
     * @return     the next byte of data, or <code>-1</code> if the end of the
     *             stream is reached.
     * @exception  IOException  if an I/O error occurs.
     */
    @Override
    public int read() throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final HttpResponse response = connectOrGetResponse();
        final InputStream is = response.getContent();
        position++;
        return is.read();
    }

    @Override
    public int read(final byte[] buffer) throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final HttpResponse response = connectOrGetResponse();
        final InputStream is = response.getContent();

        final int totalRead = is.read(buffer);

        if (totalRead > -1) {
            position += totalRead;
        }

        return totalRead;
    }

    @Override
    public int read(final byte[] buffer, final int offset, final int length)
            throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final HttpResponse response = connectOrGetResponse();
        final InputStream is = response.getContent();

        final int totalRead = is.read(buffer, offset, length);

        if (totalRead > -1) {
            position += totalRead;
        }

        return totalRead;
    }

    @Override
    public long skip(final long noOfBytesToSkip) throws IOException {
        if (!open) {
            return 0;
        }

        final HttpResponse response = connectOrGetResponse();
        final InputStream is = response.getContent();

        final long totalSkipped = is.skip(noOfBytesToSkip);

        position += totalSkipped;

        return totalSkipped;
    }



    @Override
    public int available() throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final HttpResponse response = connectOrGetResponse();
        final InputStream is = response.getContent();

        return is.available();
    }

    @Override
    public int write(final ByteBuffer src) throws IOException {
        // This is a read-only channel
        throw new NonWritableChannelException();
    }

    @Override
    public long position() throws IOException {
        return position;
    }

    @Override
    public SeekableByteChannel position(final long newPosition) throws IOException {
        return new MantaSeekableByteChannel(new AtomicReference<>(),
                objectUri, newPosition, httpRequestFactory);
    }

    @Override
    public long size() throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final HttpResponse response = connectOrGetResponse();
        final HttpHeaders headers = response.getHeaders();

        if (headers.getContentLength() == null) {
            throw new MantaClientException("Can't get SeekableByteChannel for objects of unknown size");
        }

        return headers.getContentLength();
    }

    @Override
    public synchronized void mark(final int readlimit) {
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public SeekableByteChannel truncate(final long newSize) throws IOException {
        throw new NonWritableChannelException();
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public synchronized void close() throws IOException {
        if (!open) {
            return;
        }

        if (responseRef.get() != null) {
            responseRef.get().disconnect();
        }

        open = false;
    }


    /**
     * Connects to the Manta API, updates the atomic reference and returns a
     * response if the atomic reference hasn't been set. Otherwise, it just returns
     * the response embedded in the atomic reference.
     *
     * @return HTTP response object
     * @throws IOException thrown when there are network problems connecting to the remote API
     */
    protected HttpResponse connectOrGetResponse() throws IOException {
        if (responseRef.get() != null) {
            return responseRef.get();
        }

        final HttpRequest request = httpRequestFactory.buildGetRequest(objectUri);
        final HttpHeaders headers = request.getHeaders();

        headers.setRange(String.format("bytes=%d-", position));

        responseRef.compareAndSet(null, request.execute());
        final HttpResponse response = responseRef.get();
        final String contentType = response.getHeaders().getContentType();

        if (MantaObjectResponse.DIRECTORY_RESPONSE_CONTENT_TYPE.equals(contentType)) {
            throw new MantaClientException("Can't get SeekableByteChannel for directory objects");
        }

        return response;
    }
}
