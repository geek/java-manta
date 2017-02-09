/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.multipart.*;
import com.joyent.manta.config.*;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.exception.ContextedRuntimeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Stream;

/*
* Usage: set the mantaUsername, privateKeyPath, publicKeyId, and multipartServer with your own values.
 */
public class ClientEncryptionServerMultipart {
    private static String mantaUsername = System.getenv().get("MANTA_USERNAME");
    private static int numberOf5MBParts = Integer.parseInt(System.getenv().getOrDefault("MANTA_PARTS", "2"));

    public static void main(String... args) {

        String privateKeyPath = System.getenv().get("MANTA_KEYPATH");
        String publicKeyId = System.getenv().get("MANTA_KEYID");
        String multipartServer = System.getenv().get("MANTA_SERVER");

        ConfigContext config = new ChainedConfigContext(
                new DefaultsConfigContext(),
                new EnvVarConfigContext(),
                new MapConfigContext(System.getProperties()))
                .setMantaURL(multipartServer)
                .setMantaUser(mantaUsername)
                .setMantaKeyPath(privateKeyPath)
                .setMantaKeyId(publicKeyId)
                .setClientEncryptionEnabled(true)
                .setEncryptionAlgorithm("AES256/CTR/NoPadding")
                .setEncryptionAuthenticationMode(EncryptionAuthenticationMode.Optional)
                .setPermitUnencryptedDownloads(false)
                .setEncryptionKeyId("simple/example")
                .setEncryptionPrivateKeyBytes(Base64.getDecoder().decode("RkZGRkZGRkJEOTY3ODNDNkM5MUUyMjIyMTExMTIyMjI="));

        try (MantaClient client = new MantaClient(config)) {
            EncryptedServerSideMultipartManager multipart = new EncryptedServerSideMultipartManager(client);
            multipartUpload(multipart);
        }
    }

    private static void multipartUpload(EncryptedServerSideMultipartManager multipart) {
        String uploadObject = "/" + mantaUsername + "/stor/multipart";


        // We catch network errors and handle them here
        try {
            MantaMetadata metadata = new MantaMetadata();
            metadata.put("e-secretkey", "My Secret Value");
            EncryptedMultipartUpload<ServerSideMultipartUpload> upload =
                    multipart.initiateUpload(uploadObject, metadata);
            ArrayList<MantaMultipartUploadTuple> parts = new ArrayList<>();

            for (int i = 0; i < numberOf5MBParts; i++) {
                MantaMultipartUploadPart part = multipart.uploadPart(upload, i + 1, RandomUtils.nextBytes(5242880));
                parts.add(part);
            }

            MantaMultipartUploadTuple[] partsArray = new MantaMultipartUploadTuple[parts.size()];
            parts.toArray(partsArray);
            Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(partsArray);
            long start = System.currentTimeMillis();
            multipart.complete(upload, partsStream);
            System.out.println("Total commit time: " + (System.currentTimeMillis() - start) + "ms");

            System.out.println(uploadObject + " is now assembled!");
        } catch (IOException e) {
            // This catch block is for general network failures
            // For example, ServerSideMultipartUpload.initiateUpload can throw an IOException

            ContextedRuntimeException exception = new ContextedRuntimeException(
                    "A network error occurred when doing a multipart upload to Manta.");
            exception.setContextValue("path", uploadObject);

            throw exception;
        }
    }
}
