package org.apache.seatunnel.connectors.seatunnel.hive.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.HashMap;
import java.util.Map;

public class HiveBaseOptions {

    public static final Option<String> TABLE_NAME =
            Options.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hive table name");

    public static final Option<String> METASTORE_URI =
            Options.key("metastore_uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hive metastore uri");

    public static final Option<String> HIVE_SITE_PATH =
            Options.key("hive_site_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The path of hive-site.xml");

    public static final Option<Map<String, String>> HADOOP_CONF =
            Options.key("hive.hadoop.conf")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription("Properties in hadoop conf");

    public static final Option<String> HADOOP_CONF_PATH =
            Options.key("hive.hadoop.conf-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The specified loading path for the 'core-site.xml', 'hdfs-site.xml' files");

    public static final Option<String> HDFS_SITE_PATH =
            Options.key("hdfs_site_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The path of hdfs-site.xml");

    public static final Option<String> KERBEROS_PRINCIPAL =
            Options.key("kerberos_principal")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("When use kerberos, we should set kerberos user principal");

    public static final Option<String> KERBEROS_KEYTAB_PATH =
            Options.key("kerberos_keytab_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("When using kerberos, We should specify the keytab path");

    public static final Option<String> REMOTE_USER =
            Options.key("remote_user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The remote user name of hdfs");

    public static final Option<String> KRB5_PATH =
            Options.key("krb5_path")
                    .stringType()
                    .defaultValue("/etc/krb5.conf")
                    .withDescription(
                            "When use kerberos, we should set krb5 path file path such as '/seatunnel/krb5.conf' or use the default path '/etc/krb5.conf'");
}
