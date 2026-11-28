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

package org.apache.seatunnel.connectors.seatunnel.bunnyinc;

/**
 * Declares the Bunny billing connection queries and their scalar selections.
 *
 * <p>Only fixed query fields are submitted to the GraphQL endpoint. Resource configuration selects
 * one operation without accepting arbitrary GraphQL or changing the requested tenant.
 */
enum BunnyIncResource {
    PLANS(
            "plans",
            "addon basePrice code contactUsLabel contactUsUrl createdAt description id "
                    + "internalNotes isVisible name position pricingDescription productId productPlanName "
                    + "selfServiceBuy selfServiceCancel selfServiceRenew updatedAt"),
    QUOTES(
            "quotes",
            "acceptedByName acceptedByTitle accountId amount amountDue applicationDate "
                    + "backdatedPeriods backdatedQuote billingDay contactId createdAt credits currencyId "
                    + "dealId discount discountValue evergreen id message name netPaymentDays notes "
                    + "number ownerId payableId periodAmount poNumber smallUnitAmountDue splitInvoice "
                    + "subtotal taxAmount taxCode updatedAt uuid"),
    TENANTS(
            "tenants",
            "accountId code createdAt id lastLogin name platformId subdomain updatedAt "
                    + "userCount utilizationMetrics"),
    ACCOUNTS(
            "accounts",
            "accountTypeId annualRevenue billingCity billingContactId billingCountry billingDay "
                    + "billingState billingStreet billingZip code createdAt currencyId description duns "
                    + "employees entityId entityUseCode fax groupId id industryId invoiceTemplateId "
                    + "linkedinUrl name netPaymentDays ownerUserId payingStatus phone shippingCity "
                    + "shippingCountry shippingState shippingStreet shippingZip taxNumber "
                    + "taxNumberValidated timezone updatedAt website"),
    CONTACTS(
            "contacts",
            "accountId code createdAt description email entityId firstName fullName id lastName "
                    + "linkedinUrl mailingCity mailingCountry mailingState mailingStreet mailingZip "
                    + "mobile phone portalAccess salutation title updatedAt"),
    ENTITIES(
            "entities",
            "abbreviation accentColor baseCurrencyId billingCity billingCountry billingState "
                    + "billingStreet billingZip brandColor createdAt customerServiceEmail emailSenderName "
                    + "emailTemplate fax fiscalYearStartMonth id invoiceNumberPrefix invoiceNumberSeq "
                    + "invoicesImageUrl isDefault name phone privacyUrl quoteNumberPrefix quoteNumberSeq "
                    + "quotesImageUrl refundPolicyUrl taxId taxType termsUrl timezone topNavImageUrl "
                    + "updatedAt website"),
    INVOICES(
            "invoices",
            "accountId amount amountDue amountPaid createdAt credits currencyId description "
                    + "dueAt id netPaymentDays number paidAt payableId poNumber portalUrl quoteId "
                    + "smallUnitAmountDue subtotal taxAmount updatedAt url uuid"),
    PAYMENTS(
            "payments",
            "accountId amount amountUnapplied baseCurrencyCash baseCurrencyId createdAt "
                    + "currencyId description id isLegacy memo receivedAt updatedAt"),
    PRODUCTS(
            "products",
            "code description everythingInPlus id internalNotes name platformId "
                    + "productCategoryId showProductNameOnLineItem"),
    INVOICE_ITEMS(
            "invoiceItems",
            "amount chargeType couponId currencyId discount id invoiceId lineText position "
                    + "price priceDecimals prorationRate quantity subtotal taxAmount taxCode vatCode"),
    TRANSACTIONS(
            "transactions",
            "accountId amount createdAt currencyId description id state transactionableId"),
    QUOTE_CHARGES(
            "quote_charges",
            "quoteCharges",
            "amount billingPeriod chargeType couponId createdAt currencyId currentQuantity "
                    + "discount id invoiceLineText kind name price priceDecimals priceListChargeId "
                    + "pricingModel prorationRate quantity quantityMax quantityMin quoteChangeId subtotal "
                    + "taxAmount taxCode tieredAveragePrice updatedAt vatCode"),
    SUBSCRIPTIONS(
            "subscriptions",
            "accountId cancelationDate: cancellationDate createdAt currencyId endDate id name "
                    + "period priceListId rampIntervalMonths startDate trialEndDate trialPeriod "
                    + "trialStartDate updatedAt"),
    ACCOUNT_BALANCES("accountBalances", "accountId balance currencyId id"),
    SUBSCRIPTION_CHARGES(
            "subscriptionCharges",
            "amount billingPeriod chargeType createdAt discount discountedPrice id "
                    + "invoiceLineText kind name periodPrice price priceDecimals priceListChargeId "
                    + "priceListId pricingModel prorationRate quantity quantityMax quantityMin "
                    + "selfServiceQuantity subscriptionId tieredAveragePrice updatedAt");

    final String id;
    final String field;
    private final String query;

    BunnyIncResource(String id, String fields) {
        this(id, id, fields);
    }

    BunnyIncResource(String id, String field, String fields) {
        this.id = id;
        this.field = field;
        this.query =
                "query SeaTunnelRead($first: Int!, $after: String) { "
                        + field
                        + "(first: $first, after: $after) { nodes { "
                        + fields
                        + " } pageInfo { startCursor endCursor hasNextPage hasPreviousPage } } }";
    }

    String query() {
        return query;
    }

    static BunnyIncResource parse(String id) {
        for (BunnyIncResource resource : values()) {
            if (resource.id.equals(id)) {
                return resource;
            }
        }
        throw BunnyIncConfig.invalid("resource must name a supported Bunny billing export");
    }
}
