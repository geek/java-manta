/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.serialization;

import com.esotericsoftware.kryo.Serializer;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.util.Objects;

/**
 * Abstract class providing reflection helper methods for use with
 * serialization.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 *
 * @param <T> type to serialize
 */
public abstract class AbstractManualSerializer<T> extends Serializer<T> {
    /**
     * Class to be serialized.
     */
    private Class<T> classReference;

    /**
     * Creates a new serializer instance for the specified class.
     *
     * @param classReference class to be serialized
     */
    public AbstractManualSerializer(final Class<T> classReference) {
        this.classReference = classReference;
    }

    /**
     * Creates a new serializer instance for the specified class.
     *
     * @param classReference class to be serialized
     * @param acceptsNull if true, this serializer will handle null values
     * @see #setAcceptsNull(boolean)
     */
    public AbstractManualSerializer(final Class<T> classReference,
                                    final boolean acceptsNull) {
        super(acceptsNull);

        this.classReference = classReference;
    }

    /**
     * * Creates a new serializer instance for the specified class.
     *
     * @param classReference class to be serialized
     * @param acceptsNull if true, this serializer will handle null values
     * @param immutable if true, copy() will return the original object
     * @see #setAcceptsNull(boolean)
     * @see #setImmutable(boolean)
     */
    public AbstractManualSerializer(final Class<T> classReference,
                                    final boolean acceptsNull,
                                    final boolean immutable) {
        super(acceptsNull, immutable);
        this.classReference = classReference;
    }

    /**
     * Gets a reference to a field on an object.
     *
     * @param fieldName field to get
     * @return reference to an object's field
     * @throws UnsupportedOperationException when the field isn't present
     */
    protected Field captureField(final String fieldName) {
        Field field = FieldUtils.getField(classReference, fieldName, true);

        if (field == null) {
            String msg = String.format("No field [%s] found on object [%s]",
                    classReference, fieldName);
            throw new UnsupportedOperationException(msg);
        }

        return field;
    }

    /**
     * Reads a field from an object.
     *
     * @param field field to read
     * @param object object to read from
     * @return field's value
     */
    protected Object readField(final Field field, final Object object) {
        Objects.requireNonNull(field, "Field must not be null");
        Objects.requireNonNull(object, "Object must not be null");

        try {
            return FieldUtils.readField(field, object, true);
        } catch (IllegalAccessException e) {
            String msg = String.format("Error reading private field from [%s] class",
                    object.getClass().getName());
            MantaClientSerializationException mcse = new MantaClientSerializationException(msg);
            mcse.setContextValue("field", field.getName());
            mcse.setContextValue("objectClass", object.getClass());
            throw mcse;
        }
    }

    /**
     * Writes a object to an object's field.
     *
     * @param field field to write to
     * @param target target object
     * @param value object to write
     */
    protected void writeField(final Field field, final Object target, final Object value) {
        Objects.requireNonNull(field, "Field must not be null");
        Objects.requireNonNull(target, "Target must not be null");

        try {
            FieldUtils.writeField(field, target, value);
        } catch (IllegalAccessException e) {
            String msg = String.format("Unable to write value [%s] to field [%s]",
                    value, field);
            MantaClientSerializationException mcse = new MantaClientSerializationException(msg);
            mcse.setContextValue("field", field.getName());
            mcse.setContextValue("targetClass", target.getClass());
            if (value == null) {
                mcse.setContextValue("valueClass", "null");
            } else {
                mcse.setContextValue("valueClass", value.getClass());
            }
            throw mcse;
        }
    }

    /**
     * Creates a new instance with the specified constructor parameters.
     *
     * @param params constructor parameters
     * @return new instance
     */
    protected T newInstance(final Object... params) {
        return ReflectionUtils.newInstance(classReference, params);
    }
}