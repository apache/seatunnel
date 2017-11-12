package org.interestinglab.waterdrop.utils;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.maxmind.db.InvalidDatabaseException;
import com.maxmind.db.Metadata;
import com.maxmind.db.NoCache;
import com.maxmind.db.NodeCache;
import com.maxmind.db.Reader;
import com.maxmind.db.Reader.FileMode;
import com.maxmind.geoip2.DatabaseProvider;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.*;

import java.beans.Transient;
import java.io.*;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;

/**
 * Instances of this class provide a reader for the GeoIP2 database format. IP
 * addresses can be looked up using the {@code get} method.
 */
public class DatabaseReader implements DatabaseProvider, Closeable, Serializable {

    private final transient Reader reader;

    private final transient ObjectMapper om;

    private DatabaseReader(Builder builder) throws IOException {
        if (builder.stream != null) {
            this.reader = new Reader(builder.stream, builder.cache);
        } else if (builder.database != null) {
            this.reader = new Reader(builder.database, builder.mode, builder.cache);
        } else {
            // This should never happen. If it does, review the Builder class
            // constructors for errors.
            throw new IllegalArgumentException(
                    "Unsupported Builder configuration: expected either File or URL");
        }
        this.om = new ObjectMapper();
        this.om.configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, false);
        this.om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                false);
        this.om.configure(
                DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        InjectableValues inject = new InjectableValues.Std().addValue(
                "locales", builder.locales);
        this.om.setInjectableValues(inject);
    }

    /**
     * <p>
     * Constructs a Builder for the DatabaseReader. The file passed to it must
     * be a valid GeoIP2 database file.
     * </p>
     * <p>
     * {@code Builder} creates instances of {@code DatabaseReader}
     * from values set by the methods.
     * </p>
     * <p>
     * Only the values set in the {@code Builder} constructor are required.
     * </p>
     */
    public static final class Builder {
        final File database;
        final InputStream stream;

        List<String> locales = Collections.singletonList("en");
        FileMode mode = FileMode.MEMORY_MAPPED;
        NodeCache cache = NoCache.getInstance();

        /**
         * @param stream the stream containing the GeoIP2 database to use.
         */
        public Builder(InputStream stream) {
            this.stream = stream;
            this.database = null;
        }

        /**
         * @param database the GeoIP2 database file to use.
         */
        public Builder(File database) {
            this.database = database;
            this.stream = null;
        }

        /**
         * @param val List of locale codes to use in name property from most
         *            preferred to least preferred.
         * @return Builder object
         */
        public Builder locales(List<String> val) {
            this.locales = val;
            return this;
        }

        /**
         * @param cache backing cache instance
         * @return Builder object
         */
        public Builder withCache(NodeCache cache) {
            this.cache = cache;
            return this;
        }

        /**
         * @param val The file mode used to open the GeoIP2 database
         * @return Builder object
         * @throws java.lang.IllegalArgumentException if you initialized the Builder with a URL, which uses
         *                                            {@link FileMode#MEMORY}, but you provided a different
         *                                            FileMode to this method.
         */
        public Builder fileMode(FileMode val) {
            if (this.stream != null && FileMode.MEMORY != val) {
                throw new IllegalArgumentException(
                        "Only FileMode.MEMORY is supported when using an InputStream.");
            }
            this.mode = val;
            return this;
        }

        /**
         * @return an instance of {@code DatabaseReader} created from the
         * fields set on this builder.
         * @throws IOException if there is an error reading the database
         */
        public DatabaseReader build() throws IOException {
            return new DatabaseReader(this);
        }
    }

    /**
     * @param ipAddress IPv4 or IPv6 address to lookup.
     * @return A <T> object with the data for the IP address
     * @throws IOException              if there is an error opening or reading from the file.
     * @throws AddressNotFoundException if the IP address is not in our database
     */
    private <T> T get(InetAddress ipAddress, Class<T> cls, boolean hasTraits,
                      String type) throws IOException, AddressNotFoundException {

        String databaseType = this.getMetadata().getDatabaseType();
        if (!databaseType.contains(type)) {
            String caller = Thread.currentThread().getStackTrace()[2]
                    .getMethodName();
            throw new UnsupportedOperationException(
                    "Invalid attempt to open a " + databaseType
                            + " database using the " + caller + " method");
        }

        ObjectNode node = jsonNodeToObjectNode(reader.get(ipAddress));

        // We throw the same exception as the web service when an IP is not in
        // the database
        if (node == null) {
            throw new AddressNotFoundException("The address "
                    + ipAddress.getHostAddress() + " is not in the database.");
        }

        ObjectNode ipNode;
        if (hasTraits) {
            if (node.has("traits")) {
                ipNode = jsonNodeToObjectNode(node.get("traits"));
            } else {
                ipNode = om.createObjectNode();
                node.set("traits", ipNode);
            }
        } else {
            ipNode = node;
        }
        ipNode.put("ip_address", ipAddress.getHostAddress());

        return this.om.treeToValue(node, cls);
    }

    private ObjectNode jsonNodeToObjectNode(JsonNode node)
            throws InvalidDatabaseException {
        if (node == null || node instanceof ObjectNode) {
            return (ObjectNode) node;
        }
        throw new InvalidDatabaseException(
                "Unexpected data type returned. The GeoIP2 database may be corrupt.");
    }

    /**
     * <p>
     * Closes the database.
     * </p>
     * <p>
     * If you are using {@code FileMode.MEMORY_MAPPED}, this will
     * <em>not</em> unmap the underlying file due to a limitation in Java's
     * {@code MappedByteBuffer}. It will however set the reference to
     * the buffer to {@code null}, allowing the garbage collector to
     * collect it.
     * </p>
     *
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void close() throws IOException {
        this.reader.close();
    }

    @Override
    public CountryResponse country(InetAddress ipAddress) throws IOException,
            GeoIp2Exception {
        return this.get(ipAddress, CountryResponse.class, true, "Country");
    }

    @Override
    public CityResponse city(InetAddress ipAddress) throws IOException,
            GeoIp2Exception {
        return this.get(ipAddress, CityResponse.class, true, "City");
    }

    /**
     * Look up an IP address in a GeoIP2 Anonymous IP.
     *
     * @param ipAddress IPv4 or IPv6 address to lookup.
     * @return a AnonymousIpResponse for the requested IP address.
     * @throws GeoIp2Exception if there is an error looking up the IP
     * @throws IOException     if there is an IO error
     */
    @Override
    public AnonymousIpResponse anonymousIp(InetAddress ipAddress) throws IOException,
            GeoIp2Exception {
        return this.get(ipAddress, AnonymousIpResponse.class, false, "GeoIP2-Anonymous-IP");
    }

    /**
     * Look up an IP address in a GeoIP2 Connection Type database.
     *
     * @param ipAddress IPv4 or IPv6 address to lookup.
     * @return a ConnectTypeResponse for the requested IP address.
     * @throws GeoIp2Exception if there is an error looking up the IP
     * @throws IOException     if there is an IO error
     */
    @Override
    public ConnectionTypeResponse connectionType(InetAddress ipAddress)
            throws IOException, GeoIp2Exception {
        return this.get(ipAddress, ConnectionTypeResponse.class, false,
                "GeoIP2-Connection-Type");
    }

    /**
     * Look up an IP address in a GeoIP2 Domain database.
     *
     * @param ipAddress IPv4 or IPv6 address to lookup.
     * @return a DomainResponse for the requested IP address.
     * @throws GeoIp2Exception if there is an error looking up the IP
     * @throws IOException     if there is an IO error
     */
    @Override
    public DomainResponse domain(InetAddress ipAddress) throws IOException,
            GeoIp2Exception {
        return this
                .get(ipAddress, DomainResponse.class, false, "GeoIP2-Domain");
    }

    /**
     * Look up an IP address in a GeoIP2 ISP database.
     *
     * @param ipAddress IPv4 or IPv6 address to lookup.
     * @return an IspResponse for the requested IP address.
     * @throws GeoIp2Exception if there is an error looking up the IP
     * @throws IOException     if there is an IO error
     */
    @Override
    public IspResponse isp(InetAddress ipAddress) throws IOException,
            GeoIp2Exception {
        return this.get(ipAddress, IspResponse.class, false, "GeoIP2-ISP");
    }

    /**
     * @return the metadata for the open MaxMind DB file.
     */
    public Metadata getMetadata() {
        return this.reader.getMetadata();
    }
}
