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


define('WP_INSTALLING', true);
define('FS_METHOD', 'direct');
require '/var/www/html/wp-load.php';
require_once ABSPATH . 'wp-admin/includes/upgrade.php';
require_once ABSPATH . 'wp-admin/includes/file.php';
require_once ABSPATH . 'wp-admin/includes/plugin.php';
wp_install('WooCommerce fixture', 'fixture-admin', 'fixture@example.test', true, '', 'fixture-password');
update_option('home', 'https://woocommerce-fixture');
update_option('siteurl', 'https://woocommerce-fixture');
update_option('timezone_string', 'America/New_York');
update_option('permalink_structure', '/%postname%/');
WP_Filesystem();
$result = unzip_file('/tmp/woocommerce.zip', WP_PLUGIN_DIR);
if (is_wp_error($result)) { fwrite(STDERR, 'WooCommerce installation failed'); exit(1); }
$result = activate_plugin('woocommerce/woocommerce.php');
if (is_wp_error($result)) { fwrite(STDERR, 'WooCommerce activation failed'); exit(1); }
flush_rewrite_rules();
