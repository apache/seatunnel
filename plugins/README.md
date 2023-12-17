# Introduction of plugins directory

This directory used to store some third party jar package dependency by connector running, such as jdbc drivers.

!!!Attention: If you use Zeta Engine, please add jar to `$SEATUNNEL_HOME/lib/` directory on each node.

## directory structure

The jar dependency  by connector need put in `plugins/${connector name}/lib/` dir.

For example jdbc driver jars need put in `${seatunnel_install_home}/plugins/jdbc/lib/`