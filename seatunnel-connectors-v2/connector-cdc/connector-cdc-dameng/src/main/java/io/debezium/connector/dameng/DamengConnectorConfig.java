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

package io.debezium.connector.dameng;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.document.Document;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.Collect;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Set;

@SuppressWarnings("MagicNumber")
public class DamengConnectorConfig extends HistorizedRelationalDatabaseConnectorConfig {

    public static final Field SERVER_NAME = RelationalDatabaseConnectorConfig.SERVER_NAME
        .withValidation(CommonConnectorConfig::validateServerNameIsDifferentFromHistoryTopicName);

    public static final Field PDB_NAME = Field.create(DATABASE_CONFIG_PREFIX + "pdb.name")
        .withDisplayName("PDB name")
        .withType(ConfigDef.Type.STRING)
        .withWidth(ConfigDef.Width.MEDIUM)
        .withImportance(ConfigDef.Importance.HIGH)
        .withDescription("Name of the pluggable database when working with a multi-tenant set-up. "
            + "The CDB name must be given via " + DATABASE_NAME.name() + " in this case.");

    public static final Field URL = Field.create(DATABASE_CONFIG_PREFIX + "url")
        .withDisplayName("Complete JDBC URL")
        .withType(ConfigDef.Type.STRING)
        .withWidth(ConfigDef.Width.LONG)
        .withImportance(ConfigDef.Importance.HIGH)
        .withValidation(DamengConnectorConfig::requiredWhenNoHostname)
        .withDescription("Complete JDBC URL as an alternative to specifying hostname, port and database provided "
            + "as a way to support alternative connection scenarios.");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
        .withDisplayName("Snapshot mode")
        .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
        .withWidth(ConfigDef.Width.SHORT)
        .withImportance(ConfigDef.Importance.LOW)
        .withDescription("The criteria for running a snapshot upon startup of the connector. "
            + "Options include: "
            + "'initial' (the default) to specify the connector should run a snapshot only when no offsets are available for the logical server name; "
            + "'schema_only' to specify the connector should run a snapshot of the schema when no offsets are available for the logical server name. ");

    public static final Field SNAPSHOT_LOCKING_MODE = Field.create("snapshot.locking.mode")
        .withDisplayName("Snapshot locking mode")
        .withEnum(SnapshotLockingMode.class, SnapshotLockingMode.SHARED)
        .withWidth(ConfigDef.Width.SHORT)
        .withImportance(ConfigDef.Importance.LOW)
        .withDescription("Controls how the connector holds locks on tables while performing the schema snapshot. The default is 'shared', "
            + "which means the connector will hold a table lock that prevents exclusive table access for just the initial portion of the snapshot "
            + "while the database schemas and other metadata are being read. The remaining work in a snapshot involves selecting all rows from "
            + "each table, and this is done using a flashback query that requires no locks. However, in some cases it may be desirable to avoid "
            + "locks entirely which can be done by specifying 'none'. This mode is only safe to use if no schema changes are happening while the "
            + "snapshot is taken.");

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT
        .withDefault(5236);

    private static final ConfigDefinition CONFIG_DEFINITION = HistorizedRelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
        .name("Dameng")
        .excluding(
            SCHEMA_INCLUDE_LIST,
            SCHEMA_EXCLUDE_LIST,
            RelationalDatabaseConnectorConfig.TABLE_IGNORE_BUILTIN,
            SERVER_NAME)
        .type(
            HOSTNAME,
            PORT,
            USER,
            PASSWORD,
            SERVER_NAME,
            DATABASE_NAME,
            PDB_NAME,
            SNAPSHOT_MODE,
            URL)
        .connector(
            SNAPSHOT_LOCKING_MODE)
        .create();

    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());
    public static ConfigDef CONFIG_DEF = CONFIG_DEFINITION.configDef();
    private final String databaseName;
    private final String pdbName;
    private final SnapshotMode snapshotMode;
    private final SnapshotLockingMode snapshotLockingMode;
    private final boolean lobEnabled = false;

    public DamengConnectorConfig(Configuration config) {
        super(DamengConnector.class,
            config,
            config.getString(SERVER_NAME),
            new SystemTablesPredicate(),
            x -> x.schema() + "." + x.table(),
            true,
            ColumnFilterMode.SCHEMA);
        this.databaseName = toUpperCase(config.getString(DATABASE_NAME));
        this.pdbName = toUpperCase(config.getString(PDB_NAME));
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
        this.snapshotLockingMode = SnapshotLockingMode.parse(config.getString(SNAPSHOT_LOCKING_MODE), SNAPSHOT_LOCKING_MODE.defaultValueAsString());
    }

    @Override
    protected HistoryRecordComparator getHistoryRecordComparator() {
        return new HistoryRecordComparator() {
            @Override
            protected boolean isPositionAtOrBefore(Document recorded, Document desired) {
                return Scn.valueOf(recorded.getString(SourceInfo.SCN_KEY))
                    .compareTo(Scn.valueOf(desired.getString(SourceInfo.SCN_KEY))) < 1;
            }
        };
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return new DamengSourceInfoStructMaker(Module.name(), Module.version(), this);
    }

    private static String toUpperCase(String property) {
        return property == null ? null : property.toUpperCase();
    }

    public static int requiredWhenNoHostname(Configuration config, Field field, Field.ValidationOutput problems) {
        // Validates that the field is required but only when an URL field is not present
        if (config.getString(HOSTNAME) == null) {
            return Field.isRequired(config, field, problems);
        }
        return 0;
    }

    private static class SystemTablesPredicate implements Tables.TableFilter {
        private static final Set<String> BUILT_IN_SCHEMAS = Collect.unmodifiableSet("SYS", "SYSDBA", "CTISYS");

        @Override
        public boolean isIncluded(TableId t) {
            return !BUILT_IN_SCHEMAS.contains(t.schema());
        }
    }

    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Perform a snapshot of data and schema upon initial startup of a connector.
         */
        INITIAL("initial", true),

        /**
         * Perform a snapshot of the schema but no data upon initial startup of a connector.
         */
        SCHEMA_ONLY("schema_only", false);

        private final String value;
        private final boolean includeData;

        private SnapshotMode(String value, boolean includeData) {
            this.value = value;
            this.includeData = includeData;
        }

        @Override
        public String getValue() {
            return value;
        }

        public boolean includeData() {
            return includeData;
        }

        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }

        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    public enum SnapshotLockingMode implements EnumeratedValue {
        /**
         * This mode will allow concurrent access to the table during the snapshot but prevents any
         * session from acquiring any table-level exclusive lock.
         */
        SHARED("shared"),

        /**
         * This mode will avoid using ANY table locks during the snapshot process.
         * This mode should be used carefully only when no schema changes are to occur.
         */
        NONE("none");

        private final String value;

        private SnapshotLockingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public boolean usesLocking() {
            return !value.equals(NONE.value);
        }

        public static SnapshotLockingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotLockingMode option : SnapshotLockingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        public static SnapshotLockingMode parse(String value, String defaultValue) {
            SnapshotLockingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }
}
