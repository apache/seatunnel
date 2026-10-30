<?php
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


require '/var/www/html/wp-load.php';
wp_set_current_user(1);
update_option('woocommerce_custom_orders_table_data_sync_enabled', 'no');
update_option('woocommerce_custom_orders_table_enabled', isset($argv[1]) && $argv[1] === 'hpos' ? 'yes' : 'no');
WC_Install::install();
foreach (wc_get_orders(['limit' => -1, 'return' => 'ids']) as $id) {
    wc_get_order($id)->delete(true);
}
update_option('woocommerce_currency', 'TND');
update_option('woocommerce_price_num_decimals', 3);
$dates = ['2026-01-01T00:00:00Z','2026-01-05T12:00:00Z','2026-01-06T12:00:00Z','2026-01-07T12:00:00Z','2026-02-01T00:00:00Z'];
foreach ($dates as $date) {
    $order = wc_create_order();
    $order->set_date_created(strtotime($date));
    $order->set_status($date === $dates[0] || $date === $dates[4] ? 'pending' : 'completed');
    $order->set_billing_email('buyer@example.test');
    $item = new WC_Order_Item_Product();
    $item->set_name('Fixture item');
    $item->set_quantity(1);
    $item->set_subtotal('12.345');
    $item->set_total('12.345');
    $order->add_item($item);
    $order->calculate_totals();
    $order->save();
}
global $wpdb;
if (!$wpdb->get_var("SELECT COUNT(*) FROM {$wpdb->prefix}woocommerce_api_keys")) {
$wpdb->insert($wpdb->prefix . 'woocommerce_api_keys', [
    'user_id' => 1, 'description' => 'Disposable read-only fixture', 'permissions' => 'read',
    'consumer_key' => wc_api_hash('ck_0000000000000000000000000000000000000001'),
    'consumer_secret' => 'cs_0000000000000000000000000000000000000002',
    'truncated_key' => '0000001',
]);
}
