/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.MantaHttpHeaders;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;

/**
 * Interface representing a multipart upload API to Manta.
 *
 * <p>Note: In version 3.0, this class was refactored to an interface.</p>
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.5.0
 *
 * @param <UPLOAD> Manta multipart upload object used to manage MPU state
 * @param <PART> Manta multipart upload part object used to manage MPU part state
 * @param <COMPLETE> Return type for the <code>complete()</code> methods
 */
public interface MantaMultipartManager<UPLOAD extends MantaMultipartUpload,
        PART extends MantaMultipartUploadPart, COMPLETE> {
    /**
     * Maximum number of parts for a single Manta object.
     * This value will be increased to 10,000 when multipart is implemented on
     * the server.
     */
    int MAX_PARTS = 1_000;

    /**
     * Lists multipart uploads that are currently in progress.
     *
     * @return stream of objects representing files being uploaded via multipart
     * @throws IOException thrown when there are network issues
     */
    Stream<UPLOAD> listInProgress() throws IOException;

    /**
     * Initializes a new multipart upload for an object.
     *
     * @param path remote path of Manta object to be uploaded
     * @return unique id for the multipart upload
     * @throws IOException thrown when there are network issues
     */
    UPLOAD initiateUpload(String path) throws IOException;

    /**
     * Initializes a new multipart upload for an object.
     *
     * @param path remote path of Manta object to be uploaded
     * @param mantaMetadata metadata to write to final Manta object
     * @return object representing the multipart upload state as initiated
     * @throws IOException thrown when there are network issues
     */
    UPLOAD initiateUpload(String path, MantaMetadata mantaMetadata) throws IOException;

    /**
     * Initializes a new multipart upload for an object.
     *
     * @param path remote path of Manta object to be uploaded
     * @param mantaMetadata metadata to write to final Manta object
     * @param httpHeaders HTTP headers to read from to write to final Manta object
     * @return object representing the multipart upload state as initiated
     * @throws IOException thrown when there are network issues
     */
    UPLOAD initiateUpload(String path,
                          MantaMetadata mantaMetadata,
                          MantaHttpHeaders httpHeaders) throws IOException;

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param upload multipart upload object
     * @param partNumber part number to identify relative location in final file
     * @param contents String contents to be written in UTF-8
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    PART uploadPart(UPLOAD upload, int partNumber, String contents) throws IOException;

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param upload multipart upload object
     * @param partNumber part number to identify relative location in final file
     * @param bytes byte array containing data of the part to be uploaded
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    PART uploadPart(UPLOAD upload, int partNumber, byte[] bytes) throws IOException;

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param upload multipart upload object
     * @param partNumber part number to identify relative location in final file
     * @param file file containing data of the part to be uploaded
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    PART uploadPart(UPLOAD upload, int partNumber, File file) throws IOException;

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param upload multipart upload object
     * @param partNumber part number to identify relative location in final file
     * @param inputStream stream providing data for part to be uploaded
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    PART uploadPart(UPLOAD upload, int partNumber, InputStream inputStream) throws IOException;

    /**
     * Retrieves information about a single part of a multipart upload.
     *
     * @param upload multipart upload object
     * @param partNumber part number to identify relative location in final file
     * @return multipart single part object or null if not found
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    PART getPart(UPLOAD upload, int partNumber) throws IOException;

    /**
     * Retrieves the state of a given Manta multipart upload.
     *
     * @param upload multipart upload object
     * @return enum representing the state / status of the multipart upload
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    MantaMultipartStatus getStatus(UPLOAD upload) throws IOException;

    /**
     * Lists the parts that have already been uploaded.
     *
     * @param upload multipart upload object
     * @return stream of parts identified by integer part number
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    Stream<PART> listParts(UPLOAD upload) throws IOException;

    /**
     * Validates that there is no part missing from the sequence.
     *
     * @param upload multipart upload object
     * @throws IOException thrown if there is a problem connecting to Manta
     * @throws MantaMultipartException thrown went part numbers aren't sequential
     */
    void validateThatThereAreSequentialPartNumbers(UPLOAD upload)
                   throws IOException, MantaMultipartException;
    /**
     * Aborts a multipart transfer.
     *
     * @param upload multipart upload object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    void abort(UPLOAD upload) throws IOException;

    /**
     * Completes a multipart transfer by assembling the parts on Manta as an
     * asynchronous operation.
     *
     * @param upload multipart upload object
     * @param parts iterable of multipart part objects
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    COMPLETE complete(UPLOAD upload, Iterable<? extends MantaMultipartUploadTuple> parts)
            throws IOException;

    /**
     * <p>Completes a multipart transfer by assembling the parts on Manta as an
     * asynchronous operation.</p>
     *
     * <p>Note: this performs a terminal operation on the partsStream and
     * thereby will close the stream.</p>
     *
     * @param upload multipart upload object
     * @param partsStream stream of multipart part objects
     * @throws java.io.IOException thrown if there is a problem connecting to Manta
     */
    COMPLETE complete(UPLOAD upload, Stream<? extends MantaMultipartUploadTuple> partsStream)
            throws IOException;
}
