/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.config.ConfigException;
import io.github.interestinglab.waterdrop.config.ConfigList;
import io.github.interestinglab.waterdrop.config.ConfigObject;
import io.github.interestinglab.waterdrop.config.ConfigOrigin;
import io.github.interestinglab.waterdrop.config.ConfigValue;
import io.github.interestinglab.waterdrop.config.ConfigValueType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deliberately shoving all the serialization code into this class instead of
 * doing it OO-style with each subclass. Seems better to have it all in one
 * place. This class implements a lame serialization format that supports
 * skipping unknown fields, so it's moderately more extensible than the default
 * Java serialization format.
 */
class SerializedConfigValue extends AbstractConfigValue implements Externalizable {

    // this is the version used by Java serialization, if it increments it's
    // essentially an ABI break and bad
    private static final long serialVersionUID = 1L;

    // this is how we try to be extensible
    static enum SerializedField {
        // represents a field code we didn't recognize
        UNKNOWN,

        // end of a list of fields
        END_MARKER,

        // Fields at the root
        ROOT_VALUE,
        ROOT_WAS_CONFIG,

        // Fields that make up a value
        VALUE_DATA,
        VALUE_ORIGIN,

        // Fields that make up an origin
        ORIGIN_DESCRIPTION,
        ORIGIN_LINE_NUMBER,
        ORIGIN_END_LINE_NUMBER,
        ORIGIN_TYPE,
        ORIGIN_URL,
        ORIGIN_COMMENTS,
        ORIGIN_NULL_URL,
        ORIGIN_NULL_COMMENTS,
        ORIGIN_RESOURCE,
        ORIGIN_NULL_RESOURCE;

        static SerializedField forInt(int b) {
            if (b < values().length)
                return values()[b];
            else
                return UNKNOWN;
        }
    };

    private static enum SerializedValueType {
        // the ordinals here are in the wire format, caution
        NULL(ConfigValueType.NULL),
        BOOLEAN(ConfigValueType.BOOLEAN),
        INT(ConfigValueType.NUMBER),
        LONG(ConfigValueType.NUMBER),
        DOUBLE(ConfigValueType.NUMBER),
        STRING(ConfigValueType.STRING),
        LIST(ConfigValueType.LIST),
        OBJECT(ConfigValueType.OBJECT);

        ConfigValueType configType;

        SerializedValueType(ConfigValueType configType) {
            this.configType = configType;
        }

        static SerializedValueType forInt(int b) {
            if (b < values().length)
                return values()[b];
            else
                return null;
        }

        static SerializedValueType forValue(ConfigValue value) {
            ConfigValueType t = value.valueType();
            if (t == ConfigValueType.NUMBER) {
                if (value instanceof ConfigInt)
                    return INT;
                else if (value instanceof ConfigLong)
                    return LONG;
                else if (value instanceof ConfigDouble)
                    return DOUBLE;
            } else {
                for (SerializedValueType st : values()) {
                    if (st.configType == t)
                        return st;
                }
            }

            throw new ConfigException.BugOrBroken("don't know how to serialize " + value);
        }
    };

    private ConfigValue value;
    private boolean wasConfig;

    // this has to be public for the Java deserializer
    public SerializedConfigValue() {
        super(null);
    }

    SerializedConfigValue(ConfigValue value) {
        this();
        this.value = value;
        this.wasConfig = false;
    }

    SerializedConfigValue(Config conf) {
        this(conf.root());
        this.wasConfig = true;
    }

    // when Java deserializer reads this object, return the contained
    // object instead.
    private Object readResolve() throws ObjectStreamException {
        if (wasConfig)
            return ((ConfigObject) value).toConfig();
        else
            return value;
    }

    private static class FieldOut {
        final SerializedField code;
        final ByteArrayOutputStream bytes;
        final DataOutput data;

        FieldOut(SerializedField code) {
            this.code = code;
            this.bytes = new ByteArrayOutputStream();
            this.data = new DataOutputStream(bytes);
        }
    }

    // this is a separate function to prevent bugs writing to the
    // outer stream instead of field.data
    private static void writeOriginField(DataOutput out, SerializedField code, Object v)
            throws IOException {
        switch (code) {
        case ORIGIN_DESCRIPTION:
            out.writeUTF((String) v);
            break;
        case ORIGIN_LINE_NUMBER:
            out.writeInt((Integer) v);
            break;
        case ORIGIN_END_LINE_NUMBER:
            out.writeInt((Integer) v);
            break;
        case ORIGIN_TYPE:
            out.writeByte((Integer) v);
            break;
        case ORIGIN_URL:
            out.writeUTF((String) v);
            break;
        case ORIGIN_RESOURCE:
            out.writeUTF((String) v);
            break;
        case ORIGIN_COMMENTS:
            @SuppressWarnings("unchecked")
            List<String> list = (List<String>) v;
            int size = list.size();
            out.writeInt(size);
            for (String s : list) {
                out.writeUTF(s);
            }
            break;
        case ORIGIN_NULL_URL: // FALL THRU
        case ORIGIN_NULL_RESOURCE: // FALL THRU
        case ORIGIN_NULL_COMMENTS:
            // nothing to write out besides code and length
            break;
        default:
            throw new IOException("Unhandled field from origin: " + code);
        }
    }

    // not private because we use it to serialize ConfigException
    static void writeOrigin(DataOutput out, SimpleConfigOrigin origin,
                            SimpleConfigOrigin baseOrigin) throws IOException {
        Map<SerializedField, Object> m;
        // to serialize a null origin, we write out no fields at all
        if (origin != null)
            m = origin.toFieldsDelta(baseOrigin);
        else
            m = Collections.emptyMap();
        for (Map.Entry<SerializedField, Object> e : m.entrySet()) {
            FieldOut field = new FieldOut(e.getKey());
            Object v = e.getValue();
            writeOriginField(field.data, field.code, v);
            writeField(out, field);
        }
        writeEndMarker(out);
    }

    // not private because we use it to deserialize ConfigException
    static SimpleConfigOrigin readOrigin(DataInput in, SimpleConfigOrigin baseOrigin)
            throws IOException {
        Map<SerializedField, Object> m = new EnumMap<SerializedField, Object>(SerializedField.class);
        while (true) {
            Object v = null;
            SerializedField field = readCode(in);
            switch (field) {
            case END_MARKER:
                return SimpleConfigOrigin.fromBase(baseOrigin, m);
            case ORIGIN_DESCRIPTION:
                in.readInt(); // discard length
                v = in.readUTF();
                break;
            case ORIGIN_LINE_NUMBER:
                in.readInt(); // discard length
                v = in.readInt();
                break;
            case ORIGIN_END_LINE_NUMBER:
                in.readInt(); // discard length
                v = in.readInt();
                break;
            case ORIGIN_TYPE:
                in.readInt(); // discard length
                v = in.readUnsignedByte();
                break;
            case ORIGIN_URL:
                in.readInt(); // discard length
                v = in.readUTF();
                break;
            case ORIGIN_RESOURCE:
                in.readInt(); // discard length
                v = in.readUTF();
                break;
            case ORIGIN_COMMENTS:
                in.readInt(); // discard length
                int size = in.readInt();
                List<String> list = new ArrayList<String>(size);
                for (int i = 0; i < size; ++i) {
                    list.add(in.readUTF());
                }
                v = list;
                break;
            case ORIGIN_NULL_URL: // FALL THRU
            case ORIGIN_NULL_RESOURCE: // FALL THRU
            case ORIGIN_NULL_COMMENTS:
                // nothing to read besides code and length
                in.readInt(); // discard length
                v = ""; // just something non-null to put in the map
                break;
            case ROOT_VALUE:
            case ROOT_WAS_CONFIG:
            case VALUE_DATA:
            case VALUE_ORIGIN:
                throw new IOException("Not expecting this field here: " + field);
            case UNKNOWN:
                // skip unknown field
                skipField(in);
                break;
            }
            if (v != null)
                m.put(field, v);
        }
    }

    private static void writeValueData(DataOutput out, ConfigValue value) throws IOException {
        SerializedValueType st = SerializedValueType.forValue(value);
        out.writeByte(st.ordinal());
        switch (st) {
        case BOOLEAN:
            out.writeBoolean(((ConfigBoolean) value).unwrapped());
            break;
        case NULL:
            break;
        case INT:
            // saving numbers as both string and binary is redundant but easy
            out.writeInt(((ConfigInt) value).unwrapped());
            out.writeUTF(((ConfigNumber) value).transformToString());
            break;
        case LONG:
            out.writeLong(((ConfigLong) value).unwrapped());
            out.writeUTF(((ConfigNumber) value).transformToString());
            break;
        case DOUBLE:
            out.writeDouble(((ConfigDouble) value).unwrapped());
            out.writeUTF(((ConfigNumber) value).transformToString());
            break;
        case STRING:
            out.writeUTF(((ConfigString) value).unwrapped());
            break;
        case LIST:
            ConfigList list = (ConfigList) value;
            out.writeInt(list.size());
            for (ConfigValue v : list) {
                writeValue(out, v, (SimpleConfigOrigin) list.origin());
            }
            break;
        case OBJECT:
            ConfigObject obj = (ConfigObject) value;
            out.writeInt(obj.size());
            for (Map.Entry<String, ConfigValue> e : obj.entrySet()) {
                out.writeUTF(e.getKey());
                writeValue(out, e.getValue(), (SimpleConfigOrigin) obj.origin());
            }
            break;
        }
    }

    private static AbstractConfigValue readValueData(DataInput in, SimpleConfigOrigin origin)
            throws IOException {
        int stb = in.readUnsignedByte();
        SerializedValueType st = SerializedValueType.forInt(stb);
        if (st == null)
            throw new IOException("Unknown serialized value type: " + stb);
        switch (st) {
        case BOOLEAN:
            return new ConfigBoolean(origin, in.readBoolean());
        case NULL:
            return new ConfigNull(origin);
        case INT:
            int vi = in.readInt();
            String si = in.readUTF();
            return new ConfigInt(origin, vi, si);
        case LONG:
            long vl = in.readLong();
            String sl = in.readUTF();
            return new ConfigLong(origin, vl, sl);
        case DOUBLE:
            double vd = in.readDouble();
            String sd = in.readUTF();
            return new ConfigDouble(origin, vd, sd);
        case STRING:
            return new ConfigString.Quoted(origin, in.readUTF());
        case LIST:
            int listSize = in.readInt();
            List<AbstractConfigValue> list = new ArrayList<AbstractConfigValue>(listSize);
            for (int i = 0; i < listSize; ++i) {
                AbstractConfigValue v = readValue(in, origin);
                list.add(v);
            }
            return new SimpleConfigList(origin, list);
        case OBJECT:
            int mapSize = in.readInt();
            Map<String, AbstractConfigValue> map = new LinkedHashMap<String, AbstractConfigValue>(mapSize);
            for (int i = 0; i < mapSize; ++i) {
                String key = in.readUTF();
                AbstractConfigValue v = readValue(in, origin);
                map.put(key, v);
            }
            return new SimpleConfigObject(origin, map);
        }
        throw new IOException("Unhandled serialized value type: " + st);
    }

    private static void writeValue(DataOutput out, ConfigValue value, SimpleConfigOrigin baseOrigin)
            throws IOException {
        FieldOut origin = new FieldOut(SerializedField.VALUE_ORIGIN);
        writeOrigin(origin.data, (SimpleConfigOrigin) value.origin(),
                baseOrigin);
        writeField(out, origin);

        FieldOut data = new FieldOut(SerializedField.VALUE_DATA);
        writeValueData(data.data, value);
        writeField(out, data);

        writeEndMarker(out);
    }

    private static AbstractConfigValue readValue(DataInput in, SimpleConfigOrigin baseOrigin)
            throws IOException {
        AbstractConfigValue value = null;
        SimpleConfigOrigin origin = null;
        while (true) {
            SerializedField code = readCode(in);
            if (code == SerializedField.END_MARKER) {
                if (value == null)
                    throw new IOException("No value data found in serialization of value");
                return value;
            } else if (code == SerializedField.VALUE_DATA) {
                if (origin == null)
                    throw new IOException("Origin must be stored before value data");
                in.readInt(); // discard length
                value = readValueData(in, origin);
            } else if (code == SerializedField.VALUE_ORIGIN) {
                in.readInt(); // discard length
                origin = readOrigin(in, baseOrigin);
            } else {
                // ignore unknown field
                skipField(in);
            }
        }
    }

    private static void writeField(DataOutput out, FieldOut field) throws IOException {
        byte[] bytes = field.bytes.toByteArray();
        out.writeByte(field.code.ordinal());
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static void writeEndMarker(DataOutput out) throws IOException {
        out.writeByte(SerializedField.END_MARKER.ordinal());
    }

    private static SerializedField readCode(DataInput in) throws IOException {
        int c = in.readUnsignedByte();
        if (c == SerializedField.UNKNOWN.ordinal())
            throw new IOException("field code " + c + " is not supposed to be on the wire");
        return SerializedField.forInt(c);
    }

    private static void skipField(DataInput in) throws IOException {
        int len = in.readInt();
        // skipBytes doesn't have to block
        int skipped = in.skipBytes(len);
        if (skipped < len) {
            // wastefully use readFully() if skipBytes didn't work
            byte[] bytes = new byte[(len - skipped)];
            in.readFully(bytes);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if (((AbstractConfigValue) value).resolveStatus() != ResolveStatus.RESOLVED)
            throw new NotSerializableException(
                    "tried to serialize a value with unresolved substitutions, need to Config#resolve() first, see API docs");
        FieldOut field = new FieldOut(SerializedField.ROOT_VALUE);
        writeValue(field.data, value, null /* baseOrigin */);
        writeField(out, field);

        field = new FieldOut(SerializedField.ROOT_WAS_CONFIG);
        field.data.writeBoolean(wasConfig);
        writeField(out, field);

        writeEndMarker(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        while (true) {
            SerializedField code = readCode(in);
            if (code == SerializedField.END_MARKER) {
                return;
            }
            
            DataInput input = fieldIn(in);
            if (code == SerializedField.ROOT_VALUE) {
                this.value = readValue(input, null /* baseOrigin */);
            } else if (code == SerializedField.ROOT_WAS_CONFIG) {
                this.wasConfig = input.readBoolean();
            }
        }
    }

    private DataInput fieldIn(ObjectInput in) throws IOException {
        byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);
        return new DataInputStream(new ByteArrayInputStream(bytes));
    }

    private static ConfigException shouldNotBeUsed() {
        return new ConfigException.BugOrBroken(SerializedConfigValue.class.getName()
                + " should not exist outside of serialization");
    }

    @Override
    public ConfigValueType valueType() {
        throw shouldNotBeUsed();
    }

    @Override
    public Object unwrapped() {
        throw shouldNotBeUsed();
    }

    @Override
    protected SerializedConfigValue newCopy(ConfigOrigin origin) {
        throw shouldNotBeUsed();
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "(value=" + value + ",wasConfig=" + wasConfig + ")";
    }

    @Override
    public boolean equals(Object other) {
        // there's no reason we will ever call this equals(), but
        // the one in AbstractConfigValue would explode due to
        // calling unwrapped() above, so we just give some
        // safe-to-call implementation to avoid breaking the
        // contract of java.lang.Object
        if (other instanceof SerializedConfigValue) {
            return canEqual(other)
                && (this.wasConfig == ((SerializedConfigValue) other).wasConfig)
                && (this.value.equals(((SerializedConfigValue) other).value));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int h = 41 * (41 + value.hashCode());
        h = 41 * (h + (wasConfig ? 1 : 0));
        return h;
    }
}
