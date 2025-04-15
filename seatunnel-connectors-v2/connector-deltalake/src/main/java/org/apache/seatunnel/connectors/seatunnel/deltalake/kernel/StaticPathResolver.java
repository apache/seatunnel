package org.apache.seatunnel.connectors.seatunnel.deltalake.kernel;

import java.util.Optional;

public class StaticPathResolver implements MetastoreResolver {

  private final String basePath;

  public StaticPathResolver(String basePath) {
    this.basePath = basePath;
  }

  @Override
  public Optional<String> resolvePath(String catalog, String database, String table) {
    return Optional.of(String.format("%s/%s/%s/%s", basePath, catalog, database, table));
  }
}
