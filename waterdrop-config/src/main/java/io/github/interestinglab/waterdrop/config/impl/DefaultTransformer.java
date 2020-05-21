/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package io.github.interestinglab.waterdrop.config.impl;

import io.github.interestinglab.waterdrop.config.ConfigValueType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Default automatic type transformations.
 */
final class DefaultTransformer {

    static AbstractConfigValue transform(AbstractConfigValue value,
            ConfigValueType requested) {
        if (value.valueType() == ConfigValueType.STRING) {
            String s = (String) value.unwrapped();
            switch (requested) {
            case NUMBER:
                try {
                    Long v = Long.parseLong(s);
                    return new ConfigLong(value.origin(), v, s);
                } catch (NumberFormatException e) {
                    // try Double
                }
                try {
                    Double v = Double.parseDouble(s);
                    return new ConfigDouble(value.origin(), v, s);
                } catch (NumberFormatException e) {
                    // oh well.
                }
                break;
            case NULL:
                if (s.equals("null"))
                    return new ConfigNull(value.origin());
                break;
            case BOOLEAN:
                if (s.equals("true") || s.equals("yes") || s.equals("on")) {
                    return new ConfigBoolean(value.origin(), true);
                } else if (s.equals("false") || s.equals("no")
                        || s.equals("off")) {
                    return new ConfigBoolean(value.origin(), false);
                }
                break;
            case LIST:
                // can't go STRING to LIST automatically
                break;
            case OBJECT:
                // can't go STRING to OBJECT automatically
                break;
            case STRING:
                // no-op STRING to STRING
                break;
            }
        } else if (requested == ConfigValueType.STRING) {
            // if we converted null to string here, then you wouldn't properly
            // get a missing-value error if you tried to get a null value
            // as a string.
            switch (value.valueType()) {
            case NUMBER: // FALL THROUGH
            case BOOLEAN:
                return new ConfigString.Quoted(value.origin(),
                        value.transformToString());
            case NULL:
                // want to be sure this throws instead of returning "null" as a
                // string
                break;
            case OBJECT:
                // no OBJECT to STRING automatically
                break;
            case LIST:
                // no LIST to STRING automatically
                break;
            case STRING:
                // no-op STRING to STRING
                break;
            }
        } else if (requested == ConfigValueType.LIST && value.valueType() == ConfigValueType.OBJECT) {
            // attempt to convert an array-like (numeric indices) object to a
            // list. This would be used with .properties syntax for example:
            // -Dfoo.0=bar -Dfoo.1=baz
            // To ensure we still throw type errors for objects treated
            // as lists in most cases, we'll refuse to convert if the object
            // does not contain any numeric keys. This means we don't allow
            // empty objects here though :-/
            AbstractConfigObject o = (AbstractConfigObject) value;
            Map<Integer, AbstractConfigValue> values = new HashMap<Integer, AbstractConfigValue>();
            for (String key : o.keySet()) {
                int i;
                try {
                    i = Integer.parseInt(key, 10);
                    if (i < 0)
                        continue;
                    values.put(i, o.get(key));
                } catch (NumberFormatException e) {
                    continue;
                }
            }
            if (!values.isEmpty()) {
                ArrayList<Map.Entry<Integer, AbstractConfigValue>> entryList = new ArrayList<Map.Entry<Integer, AbstractConfigValue>>(
                        values.entrySet());
                // sort by numeric index
                Collections.sort(entryList,
                        new Comparator<Map.Entry<Integer, AbstractConfigValue>>() {
                            @Override
                            public int compare(Map.Entry<Integer, AbstractConfigValue> a,
                                               Map.Entry<Integer, AbstractConfigValue> b) {
                                return Integer.compare(a.getKey(), b.getKey());
                            }
                        });
                // drop the indices (we allow gaps in the indices, for better or
                // worse)
                ArrayList<AbstractConfigValue> list = new ArrayList<AbstractConfigValue>();
                for (Map.Entry<Integer, AbstractConfigValue> entry : entryList) {
                    list.add(entry.getValue());
                }
                return new SimpleConfigList(value.origin(), list);
            }
        }

        return value;
    }
}
