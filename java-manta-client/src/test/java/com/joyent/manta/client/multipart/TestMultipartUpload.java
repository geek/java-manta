package com.joyent.manta.client.multipart;

import java.io.File;
import java.util.UUID;

public class TestMultipartUpload extends AbstractMultipartUpload {
    private final File partsDirectory;
    private final File destinationDirectory;
    private final File metadata;
    private final File headers;
    private final File contents;
    private final Long contentLength;

    public TestMultipartUpload(UUID uploadId,
                               String path,
                               Long contentLength,
                               File partsDirectory,
                               File destinationDirectory,
                               File metadata, File headers,
                               File contents) {
        super(uploadId, path);
        this.contentLength = contentLength;
        this.partsDirectory = partsDirectory;
        this.destinationDirectory = destinationDirectory;
        this.metadata = metadata;
        this.headers = headers;
        this.contents = contents;
    }

    public File getMetadata() {
        return metadata;
    }

    public File getHeaders() {
        return headers;
    }

    public File getContents() {
        return contents;
    }

    public Long getContentLength() {
        return contentLength;
    }

    public File getPartsPath() {
        return new File(partsDirectory.getPath() +
            File.separator + getId());
    }

    public File getDestinationPath() {
        return new File(destinationDirectory + File.separator + getPath());
    }
}
