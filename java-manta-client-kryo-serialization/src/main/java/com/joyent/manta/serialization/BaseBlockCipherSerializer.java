/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.bouncycastle.crypto.BufferedBlockCipher;
import org.bouncycastle.crypto.engines.AESFastEngine;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.modes.SICBlockCipher;
import org.bouncycastle.crypto.params.AEADParameters;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.params.ParametersWithIV;
import org.bouncycastle.jcajce.provider.symmetric.AES;
import org.bouncycastle.jcajce.provider.symmetric.util.BaseBlockCipher;
import org.bouncycastle.jcajce.provider.symmetric.util.BaseWrapCipher;
import org.bouncycastle.jcajce.provider.symmetric.util.BlockCipherProvider;
import org.bouncycastle.jcajce.util.BCJcaJceHelper;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.objenesis.instantiator.sun.MagicInstantiator;

import java.lang.reflect.Field;
import java.security.AlgorithmParameters;

/**
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class BaseBlockCipherSerializer<T extends BaseBlockCipher> extends Serializer<T> {
    private final Field aeadParamsField;
    private final Field availableSpecsField;
    private final Field baseEngineField;
    private final Field cipherField;
    private final Field digestField;
    private final Field engineParamsField;
    private final Field engineProviderField;
    private final Field fixedIvField;
    private final Field helperField;
    private final Field ivField;
    private final Field ivLengthField;
    private final Field ivParamField;
    private final Field ivSizeField;
    private final Field keySizeInBitsField;
    private final Field modeNameField;
    private final Field baseWrapCipherAvailableSpecsField;
    private final Field paddedField;
    private final Field pbeAlgorithmField;
    private final Field pbeHashField;
    private final Field pbeIvSizeField;
    private final Field pbeKeySizeField;
    private final Field pbeSpecField;
    private final Field pbeTypeField;
    private final Field schemeField;
    private final Field wrapEngineField;

    private final Class<T> serializeClass;

    public BaseBlockCipherSerializer(final Kryo kryo, final Class<T> serializeClass) {
        super(false);
        registerClasses(kryo);

        this.serializeClass = serializeClass;

        this.aeadParamsField = captureField("aeadParams");
        this.availableSpecsField = captureField("availableSpecs");
        this.baseEngineField = captureField("baseEngine");
        this.cipherField = captureField("cipher");
        this.digestField = captureField("digest");
        this.engineParamsField = captureField("engineParams");
        this.engineProviderField = FieldUtils.getField(BaseBlockCipher.class, "engineProvider", true);
        this.fixedIvField = captureField("fixedIv");
        this.helperField = captureField("helper");
        this.ivField = captureField("iv");
        this.ivLengthField = captureField("ivLength");
        this.ivParamField = captureField("ivParam");
        this.ivSizeField = captureField("ivSize");
        this.keySizeInBitsField = captureField("keySizeInBits");
        this.modeNameField = captureField("modeName");
        this.baseWrapCipherAvailableSpecsField =  FieldUtils.getField(
                BaseWrapCipher.class, "availableSpecs", true);
        this.paddedField = captureField("padded");
        this.pbeAlgorithmField = captureField("pbeAlgorithm");
        this.pbeHashField = captureField("pbeHash");
        this.pbeIvSizeField = captureField("pbeIvSize");
        this.pbeKeySizeField = captureField("pbeKeySize");
        this.pbeSpecField = captureField("pbeSpec");
        this.pbeTypeField = captureField("pbeType");
        this.schemeField = captureField("scheme");
        this.wrapEngineField = captureField("wrapEngine");
    }

    private void registerClasses(final Kryo kryo) {
        final Class<?> bufferedGenericBlockCipherClass = findClass(
                "org.bouncycastle.jcajce.provider.symmetric.util.BaseBlockCipher$BufferedGenericBlockCipher");

        final Class<?> paddedBufferedBlockCipherClass = findClass(
                "org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher");

        kryo.register(AESFastEngine.class, new CompatibleFieldSerializer(
                kryo, AESFastEngine.class));
        kryo.register(BufferedBlockCipher.class);
        kryo.register(bufferedGenericBlockCipherClass, new CompatibleFieldSerializer(kryo, bufferedGenericBlockCipherClass))
                .setInstantiator(new MagicInstantiator<>(bufferedGenericBlockCipherClass));
        kryo.register(SICBlockCipher.class)
                .setInstantiator(new MagicInstantiator<>(SICBlockCipher.class));
        kryo.register(AlgorithmParameters.class);
        kryo.register(BCJcaJceHelper.class);
        kryo.register(BouncyCastleProvider.class, new BouncyCastleProviderSerializer());
        kryo.register(ParametersWithIV.class)
                .setInstantiator(new MagicInstantiator<>(ParametersWithIV.class));
        kryo.register(KeyParameter.class)
                .setInstantiator(new MagicInstantiator<>(KeyParameter.class));
        kryo.register(paddedBufferedBlockCipherClass)
                .setInstantiator(new MagicInstantiator<>(paddedBufferedBlockCipherClass));
        kryo.register(CBCBlockCipher.class)
                .setInstantiator(new MagicInstantiator<>(CBCBlockCipher.class));
        kryo.register(AEADParameters.class)
                .setInstantiator(new MagicInstantiator<>(AEADParameters.class));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(final Kryo kryo, final Output output, final T object) {
        kryo.writeObjectOrNull(output, readField(aeadParamsField, object), AEADParameters.class);
        kryo.writeClassAndObject(output, readField(availableSpecsField, object));
        kryo.writeClassAndObject(output, readField(baseEngineField, object));
        kryo.writeClassAndObject(output, readField(cipherField, object));
        output.writeVarInt((Integer)readField(digestField, object), true);

        if (readField(engineParamsField, object) != null) {
            throw new UnsupportedOperationException("We don't support "
                    + "serializing the engineParams field");
        }

        output.writeBoolean((Boolean)readField(fixedIvField, object));

        // helper does not need to be serialized

        final byte[] iv = (byte[])readField(ivField, object);
        kryo.writeObjectOrNull(output, iv, byte[].class);
        output.writeVarInt((Integer)readField(ivLengthField, object), true);
        kryo.writeClassAndObject(output, readField(ivParamField, object));
        output.writeVarInt((Integer)readField(ivSizeField, object), true);
        output.writeVarInt((Integer)readField(keySizeInBitsField, object), true);
        output.writeString((String)readField(modeNameField, object));
        kryo.writeClassAndObject(output, readField(baseWrapCipherAvailableSpecsField, object));
        output.writeBoolean((Boolean)readField(paddedField, object));
        output.writeString((String)readField(pbeAlgorithmField, object));
        output.writeVarInt((Integer)readField(pbeHashField, object), true);
        output.writeVarInt((Integer)readField(pbeIvSizeField, object), true);
        output.writeVarInt((Integer)readField(pbeKeySizeField, object), true);
        kryo.writeClassAndObject(output, readField(pbeSpecField, object));
        output.writeVarInt((Integer)readField(pbeTypeField, object), true);
        output.writeVarInt((Integer)readField(schemeField, object), false);
        kryo.writeClassAndObject(output, readField(wrapEngineField, object));

        output.flush();
    }

    @Override
    public T read(final Kryo kryo, final Input input, final Class<T> type) {
        final AEADParameters aeadParams = kryo.readObjectOrNull(input, AEADParameters.class);
        final Object availableSpecs = kryo.readClassAndObject(input);
        final Object baseEngine = kryo.readClassAndObject(input);
        final Object cipher = kryo.readClassAndObject(input);
        final int digest = input.readVarInt(true);
        final BlockCipherProvider engineProvider = buildEngineProvider();

        final boolean fixedIv = input.readBoolean();
        final BCJcaJceHelper helper = new BCJcaJceHelper();

        final byte[] iv = kryo.readObjectOrNull(input, byte[].class);
        final int ivLength = input.readVarInt(true);
        final Object ivParam = kryo.readClassAndObject(input);
        final int ivSize = input.readVarInt(true);
        final int keySizeInBits = input.readVarInt(true);
        final String modeName = input.readString();
        final Object baseWrapCipherAvailableSpecs = kryo.readClassAndObject(input);
        final boolean padded = input.readBoolean();
        final String pbeAlgorithm = input.readString();
        final int pbeHash = input.readVarInt(true);
        final int pbeIvSize = input.readVarInt(true);
        final int pbeKeySize = input.readVarInt(true);
        final Object pbeSpec = kryo.readClassAndObject(input);
        final int pbeType = input.readVarInt(true);
        final int scheme = input.readVarInt(false);
        final Object wrapEngine = kryo.readClassAndObject(input);

        final T instance;
        try {
             instance = serializeClass.newInstance();
        } catch (ReflectiveOperationException e) {
            String msg = String.format("Unable to create new instance of class "
                            + "[%s]", serializeClass.getName());
            throw new SerializationException(msg, e);
        }

        writeField(aeadParamsField, instance, aeadParams);
        writeField(availableSpecsField, instance, availableSpecs);
        writeField(baseEngineField, instance, baseEngine);
        writeField(cipherField, instance, cipher);
        writeField(digestField, instance, digest);
        writeField(engineProviderField, instance, engineProvider);
        writeField(fixedIvField, instance, fixedIv);
        writeField(helperField, instance, helper);
        writeField(ivField, instance, iv);
        writeField(ivLengthField, instance, ivLength);
        writeField(ivParamField, instance, ivParam);
        writeField(ivSizeField, instance, ivSize);
        writeField(keySizeInBitsField, instance, keySizeInBits);
        writeField(modeNameField, instance, modeName);
        writeField(baseWrapCipherAvailableSpecsField, instance, baseWrapCipherAvailableSpecs);
        writeField(paddedField, instance, padded);
        writeField(pbeAlgorithmField, instance, pbeAlgorithm);
        writeField(pbeHashField, instance, pbeHash);
        writeField(pbeIvSizeField, instance, pbeIvSize);
        writeField(pbeKeySizeField, instance, pbeKeySize);
        writeField(pbeSpecField, instance, pbeSpec);
        writeField(pbeTypeField, instance, pbeType);
        writeField(schemeField, instance, scheme);
        writeField(wrapEngineField, instance, wrapEngine);


        return instance;
    }


    @SuppressWarnings("unchecked")
    private BlockCipherProvider buildEngineProvider() {
        final String name = this.serializeClass.getSimpleName();

        final BaseBlockCipher baseBlockCipher;

        if (name.equals("ECB")) {
            baseBlockCipher = new AES.ECB();
        } else if (name.equals("CBC")) {
            baseBlockCipher = new AES.CBC();
        } else if (name.equals("CFB")) {
            baseBlockCipher = new AES.CFB();
        } else if (name.equals("OFB")) {
            baseBlockCipher = new AES.OFB();
        } else if (name.equals("GCM")) {
            baseBlockCipher = new AES.GCM();
        } else if (name.equals("CCM")) {
            baseBlockCipher = new AES.CCM();
        } else {
            String msg = String.format("Can't generate engine provider because "
                    + "BaseBlockCipher is an unknown implementation: %s",
                    this.serializeClass.getName());
            throw new UnsupportedOperationException(msg);
        }

        return (BlockCipherProvider)readField(
                engineProviderField, baseBlockCipher);
    }

    private Field captureField(final String fieldName) {
        return FieldUtils.getField(serializeClass, fieldName, true);
    }

    private Object readField(final Field field, Object object) {
        try {
            return FieldUtils.readField(field, object, true);
        } catch (IllegalAccessException e) {
            String msg = String.format("Error reading private field from [%s] class",
                    object.getClass().getName());
            throw new SerializationException(msg);
        }
    }

    private void writeField(final Field field, final Object target, final Object value) {
        try {
            FieldUtils.writeField(field, target, value);
        } catch (IllegalAccessException e) {
            String msg = String.format("Unable to write value [%s] to field [%s]",
                    value, field);
            throw new SerializationException(msg, e);
        }
    }

    private static Class<?> findClass(final String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            String msg = String.format("Class not found in class path: %s",
                    className);
            throw new UnsupportedOperationException(msg, e);
        }
    }
}
