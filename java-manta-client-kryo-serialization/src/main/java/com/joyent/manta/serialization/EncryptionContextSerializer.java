/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.joyent.manta.client.crypto.AbstractAesCipherDetails;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedCiphersLookupMap;
import com.joyent.manta.client.multipart.EncryptionState;
import org.bouncycastle.crypto.params.KeyParameter;
import org.objenesis.instantiator.sun.MagicInstantiator;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.lang.reflect.Field;

/**
 * Kryo serializer that deconstructs a {@link EncryptionContext} class for serialization / deserialization.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class EncryptionContextSerializer extends AbstractManualSerializer<EncryptionContext> {

    public EncryptionContextSerializer(final Kryo kryo) {
        super(EncryptionContext.class, false);
        registerClasses(kryo);
    }

    private void registerClasses(final Kryo kryo) {
        kryo.register(Cipher.class, new CipherSerializer(kryo));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(final Kryo kryo, final Output output, final EncryptionContext object) {
        kryo.writeObject(output, object.getCipherDetails().getCipherId());

        output.flush();
    }

    @Override
    public EncryptionContext read(final Kryo kryo, final Input input, final Class<EncryptionContext> type) {
        final String cipherId = kryo.readObject(input, String.class);
        final Object cipher = kryo.readObject(input, Cipher.class);
        SupportedCipherDetails cipherDetails = SupportedCiphersLookupMap.INSTANCE.get(cipherId);

        final EncryptionContext encryptionContext = new EncryptionContext(cipherDetails);

        return encryptionContext;
    }
}
