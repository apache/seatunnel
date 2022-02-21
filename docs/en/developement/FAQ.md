# Development FAQ:

1. Q: Import project, compiler has exception "class not found **org.apache.seatunnel.shade.com.typesafe.config.Config**"

   A: Run "mvn install" first.
      Because in the "seatunnel-config/seatunnel-config-base" subproject, package "com.typesafe.config" has been relocated to "org.apache.seatunnel.shade.com.typesafe.config" and install to maven local repository in subproject "seatunnel-config/seatunnel-config-shade".
