package org.apache.seatunnel.connectors.seatunnel.deltalake.kernel;

import java.util.Optional;

public interface MetastoreResolver {
  Optional<String> resolvePath(String catalog, String database, String table);
}

