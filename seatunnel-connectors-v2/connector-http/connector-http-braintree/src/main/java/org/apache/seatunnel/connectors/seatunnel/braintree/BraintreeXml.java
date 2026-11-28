/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.connectors.seatunnel.braintree;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.DecimalNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Parses the Braintree v6 XML document under structural and numeric bounds.
 *
 * <p>The gateway's scalar types and nested fields become JSON. No DTD or external entity resolution
 * is available to the provider document.
 */
final class BraintreeXml {
    private static final JsonNodeFactory NODES = JsonNodeFactory.instance;
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final Set<String> MONEY_AND_COUNTS =
            Set.of(
                    "amount",
                    "amount_disputed",
                    "amount_won",
                    "balance",
                    "price",
                    "discount_amount",
                    "shipping_amount",
                    "shipping_tax_amount",
                    "tax_amount",
                    "service_fee_amount",
                    "surcharge_amount",
                    "settlement_amount",
                    "settlement_currency_exchange_rate",
                    "settlement_base_currency_exchange_rate",
                    "next_bill_amount",
                    "next_billing_period_amount",
                    "quantity",
                    "current_billing_cycle",
                    "number_of_billing_cycles",
                    "billing_frequency",
                    "billing_day_of_month",
                    "trial_duration",
                    "days_past_due",
                    "failure_count",
                    "installment_count");
    private static final Set<String> BOOLEAN_FIELDS =
            Set.of(
                    "default",
                    "expired",
                    "never_expires",
                    "trial_period",
                    "success",
                    "funds_held",
                    "tax_exempt",
                    "recurring",
                    "pin_verified",
                    "processed_with_network_token",
                    "verified",
                    "verifiable");
    private static final Set<String> DATES =
            Set.of(
                    "received_date",
                    "reply_by_date",
                    "effective_date",
                    "disbursement_date",
                    "next_billing_date",
                    "first_billing_date",
                    "billing_period_start_date",
                    "billing_period_end_date",
                    "paid_through_date");
    private static final Set<String> TIMES =
            Set.of(
                    "created_at",
                    "updated_at",
                    "revoked_at",
                    "timestamp",
                    "authorization_expires_at",
                    "sent_to_processor_at",
                    "send_at",
                    "plaid_verified_at");
    private static final Set<String> LISTS =
            Set.of(
                    "discounts",
                    "add_ons",
                    "transactions",
                    "disputes",
                    "addresses",
                    "payment_methods",
                    "status_history",
                    "refund_ids",
                    "refund_global_ids",
                    "paypal_messages",
                    "credit_cards",
                    "paypal_accounts",
                    "apple_pay_cards",
                    "android_pay_cards",
                    "samsung_pay_cards",
                    "venmo_accounts",
                    "visa_checkout_cards",
                    "masterpass_cards",
                    "us_bank_accounts");
    private static final List<String> PAYMENT_LISTS =
            List.of(
                    "credit_cards",
                    "paypal_accounts",
                    "apple_pay_cards",
                    "android_pay_cards",
                    "samsung_pay_cards",
                    "venmo_accounts",
                    "visa_checkout_cards",
                    "masterpass_cards",
                    "us_bank_accounts");

    private BraintreeXml() {}

    /**
     * Reads one complete document into bounded provider elements.
     *
     * <p>Typed records are projected only after the root and XML structure are accepted.
     */
    static Element read(
            InputStream input,
            BraintreeConfig config,
            BraintreeResource resource,
            Deadline deadline)
            throws XMLStreamException, InterruptedException {
        XMLInputFactory factory = XMLInputFactory.newDefaultFactory();
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        factory.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, false);
        factory.setXMLResolver(
                (publicId, systemId, base, namespace) -> {
                    throw new XMLStreamException("External resolution is disabled");
                });
        XMLStreamReader parser = factory.createXMLStreamReader(input);
        Deque<Element> stack = new ArrayDeque<>();
        Element root = null;
        int count = 0;
        try {
            while (parser.hasNext()) {
                int event = parser.next();
                deadline.check();
                if (event == XMLStreamConstants.DTD
                        || event == XMLStreamConstants.ENTITY_REFERENCE) {
                    throw resource.failure("DTD and entity declarations are forbidden");
                }
                if (event == XMLStreamConstants.START_ELEMENT) {
                    if (stack.size() == config.xmlDepth || ++count > config.xmlNodes) {
                        throw resource.failure("XML exceeds max_xml_depth or max_xml_nodes");
                    }
                    Element element = element(parser, resource);
                    if (stack.isEmpty()) {
                        if (root != null) {
                            throw resource.failure("multiple XML roots");
                        }
                        root = element;
                    } else {
                        stack.peek().children.add(element);
                    }
                    stack.push(element);
                } else if (event == XMLStreamConstants.END_ELEMENT) {
                    stack.pop();
                } else if ((event == XMLStreamConstants.CHARACTERS
                                || event == XMLStreamConstants.CDATA)
                        && !stack.isEmpty()) {
                    stack.peek()
                            .value
                            .append(
                                    parser.getTextCharacters(),
                                    parser.getTextStart(),
                                    parser.getTextLength());
                }
            }
            if (root == null || !stack.isEmpty()) {
                throw resource.failure("XML document has no complete root");
            }
            if (root.name.equals("api-error-response") || root.name.equals("errors")) {
                throw resource.failure("gateway returned an XML error envelope");
            }
            return root;
        } finally {
            parser.close();
        }
    }

    private static Element element(XMLStreamReader parser, BraintreeResource resource) {
        String type = parser.getAttributeValue(null, "type");
        String plainNil = parser.getAttributeValue(null, "nil");
        String qualifiedNil =
                parser.getAttributeValue("http://www.w3.org/2001/XMLSchema-instance", "nil");
        if (plainNil != null && qualifiedNil != null && !plainNil.equals(qualifiedNil)) {
            throw resource.failure("conflicting XML nil attributes");
        }
        String nil = plainNil != null ? plainNil : qualifiedNil;
        boolean missing = nil != null && bool(nil, resource);
        return new Element(parser.getLocalName(), type == null ? "" : type, missing);
    }

    static ObjectNode record(Element element, BraintreeResource resource) {
        JsonNode value = value(element, key(element.name), resource);
        if (!value.isObject()) {
            throw resource.failure("record XML must contain an object");
        }
        return (ObjectNode) value;
    }

    private static JsonNode value(Element elem, String name, BraintreeResource resource) {
        if (elem.nil) {
            if (!elem.children.isEmpty() || !elem.value.toString().isBlank()) {
                throw resource.failure("nil XML element contains data");
            }
            return NODES.nullNode();
        }
        if (elem.type.equals("array") || elem.type.isEmpty() && LISTS.contains(name)) {
            if (!elem.value.toString().isBlank()) {
                throw resource.failure("array XML element has unexpected text");
            }
            ArrayNode output = NODES.arrayNode();
            boolean mixed =
                    elem.children.stream().map(child -> child.name).distinct().limit(2).count() > 1;
            for (Element item : elem.children) {
                String child = key(item.name);
                JsonNode converted = value(item, name.endsWith("_ids") ? "id" : child, resource);
                output.add(mixed ? NODES.objectNode().set(child, converted) : converted);
            }
            return output;
        }
        if (!elem.children.isEmpty() || Set.of("hash", "map", "object").contains(elem.type)) {
            ObjectNode output = NODES.objectNode();
            Map<String, String> firstNames = new HashMap<>();
            Set<String> repeated = new HashSet<>();
            for (Element child : elem.children) {
                String field = key(child.name);
                String original = firstNames.putIfAbsent(field, child.name);
                if (original != null && !original.equals(child.name)) {
                    throw resource.failure("XML element names collide after normalization");
                }
                JsonNode node = value(child, field, resource);
                if (original == null) {
                    output.set(field, node);
                } else if (repeated.add(field)) {
                    output.set(field, NODES.arrayNode().add(output.get(field)).add(node));
                } else {
                    ((ArrayNode) output.get(field)).add(node);
                }
            }
            if (!elem.value.toString().isBlank()) {
                if (output.has("_text")) {
                    throw resource.failure("XML mixed text collides with a named field");
                }
                output.put("_text", elem.value.toString());
            }
            knownObjects(name, output);
            return output;
        }
        return scalar(elem, name, resource);
    }

    private static JsonNode scalar(Element elem, String name, BraintreeResource resource) {
        String raw = elem.value.toString();
        if (name.equals("id")
                || name.endsWith("_id")
                || name.equals("bin")
                || name.equals("last_4")
                || name.endsWith("_number")
                || name.equals("country_code_numeric")) {
            return NODES.textNode(raw);
        }
        String kind = elem.type;
        if (kind.isEmpty() && !raw.isBlank()) {
            if (MONEY_AND_COUNTS.contains(name)) {
                kind = "decimal";
            } else if (BOOLEAN_FIELDS.contains(name)) {
                kind = "boolean";
            } else if (DATES.contains(name)) {
                kind = "date";
            } else if (TIMES.contains(name)) {
                kind = "datetime";
            }
        }
        String text = raw.trim();
        try {
            switch (kind) {
                case "integer":
                case "long":
                    if (text.length() > 1024) {
                        throw resource.failure("XML integer is oversized");
                    }
                    return NODES.numberNode(new BigInteger(text));
                case "decimal":
                case "float":
                case "double":
                    if (text.length() > 1024) {
                        throw resource.failure("XML decimal is oversized");
                    }
                    BigDecimal value = new BigDecimal(text);
                    if (Math.abs((long) value.scale()) > 1024L) {
                        throw resource.failure("XML decimal exponent is oversized");
                    }
                    return new DecimalNode(value);
                case "boolean":
                    return NODES.booleanNode(bool(text, resource));
                case "date":
                    return NODES.textNode(LocalDate.parse(text).toString());
                case "dateTime":
                case "datetime":
                    return NODES.textNode(instant(text, resource).toString());
                default:
                    return NODES.textNode(raw);
            }
        } catch (DateTimeException | NumberFormatException error) {
            throw resource.failure("typed XML scalar is invalid");
        }
    }

    static Instant instant(String value, BraintreeResource resource) {
        try {
            return OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeException ex) {
            throw resource.failure("invalid XML datetime");
        }
    }

    private static boolean bool(String text, BraintreeResource resource) {
        if ("true".equalsIgnoreCase(text) || text.equals("1")) {
            return true;
        }
        if ("false".equalsIgnoreCase(text) || text.equals("0")) {
            return false;
        }
        throw resource.failure("invalid XML boolean");
    }

    private static String key(String name) {
        return name.replace('-', '_').toLowerCase(Locale.ROOT);
    }

    private static void knownObjects(String name, ObjectNode data) {
        if (name.equals("merchant_account") || name.equals("master_merchant_account")) {
            alias(data, "business", "business_details");
            alias(data, "individual", "individual_details");
            alias(data, "funding", "funding_details");
            for (String field : List.of("business_details", "individual_details")) {
                JsonNode nested = data.path(field);
                if (nested.isObject()) {
                    alias((ObjectNode) nested, "address", "address_details");
                }
            }
        }
        if (name.equals("transaction")) {
            alias(data, "billing", "billing_details");
            alias(data, "shipping", "shipping_details");
            alias(data, "customer", "customer_details");
            alias(data, "credit_card", "credit_card_details");
            alias(data, "paypal", "paypal_details");
            alias(data, "apple_pay", "apple_pay_details");
            alias(data, "android_pay_card", "android_pay_card_details");
            alias(data, "samsung_pay_card", "samsung_pay_card_details");
            alias(data, "masterpass_card", "masterpass_card_details");
            alias(data, "venmo_account", "venmo_account_details");
            alias(data, "visa_checkout_card", "visa_checkout_card_details");
        }
        if (Set.of("customer", "dispute", "subscription", "transaction").contains(name)
                && data.has("global_id")
                && !data.has("graphql_id")) {
            data.set("graphql_id", data.get("global_id"));
        }
        if (name.equals("customer") && !data.has("payment_methods")) {
            ArrayNode methods = NODES.arrayNode();
            for (String field : PAYMENT_LISTS) {
                JsonNode collection = data.path(field);
                if (collection.isArray()) {
                    methods.addAll((ArrayNode) collection);
                }
            }
            data.set("payment_methods", methods);
        }
    }

    private static void alias(ObjectNode data, String old, String current) {
        if (data.has(old) && !data.has(current)) {
            data.set(current, data.remove(old));
        }
    }

    static String encode(JsonNode value, int limit, BraintreeResource resource) {
        try (BoundedOutput output = new BoundedOutput(limit, resource)) {
            JSON.writeValue(output, value);
            return output.toString(StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw resource.failure("could not encode the XML record");
        }
    }

    /**
     * A bounded provider XML element before resource and field normalization.
     *
     * <p>Native tag case, typed text and hierarchy are available to collection parsing.
     */
    static final class Element {
        final String name;
        final String type;
        final boolean nil;
        final StringBuilder value = new StringBuilder();
        final List<Element> children = new ArrayList<>();

        Element(String name, String type, boolean nil) {
            this.name = name;
            this.type = type;
            this.nil = nil;
        }

        Element single(String tag, BraintreeResource resource) {
            Element match = null;
            for (Element child : children) {
                if (child.name.equals(tag)) {
                    if (match != null) {
                        throw resource.failure("duplicate XML collection metadata");
                    }
                    match = child;
                }
            }
            if (match == null || match.nil) {
                throw resource.failure("missing XML collection metadata");
            }
            return match;
        }

        int number(String tag, BraintreeResource resource) {
            Element field = single(tag, resource);
            String text = field.value.toString().trim();
            if (!field.children.isEmpty() || !text.matches("[0-9]{1,10}")) {
                throw resource.failure("invalid XML collection metadata");
            }
            try {
                return Integer.parseInt(text);
            } catch (NumberFormatException ex) {
                throw resource.failure("invalid XML collection metadata");
            }
        }
    }

    /**
     * Checks the HTTPS deadline while accepting XML events.
     *
     * <p>The transport can close the underlying socket while input is blocked.
     */
    @FunctionalInterface
    interface Deadline {
        void check() throws InterruptedException;
    }

    /**
     * Caps projected JSON which can be larger than the original XML scalar representation.
     *
     * <p>Derived fields cannot make a single emitted record exceed the configured byte bound.
     */
    private static final class BoundedOutput extends ByteArrayOutputStream {
        private final int limit;
        private final BraintreeResource resource;

        BoundedOutput(int limit, BraintreeResource resource) {
            this.limit = limit;
            this.resource = resource;
        }

        @Override
        public synchronized void write(int value) {
            allow(1);
            super.write(value);
        }

        @Override
        public synchronized void write(byte[] value, int offset, int length) {
            allow(length);
            super.write(value, offset, length);
        }

        private void allow(int length) {
            if (length > limit - size()) {
                throw resource.failure("projected XML record exceeds max_response_bytes");
            }
        }
    }
}
