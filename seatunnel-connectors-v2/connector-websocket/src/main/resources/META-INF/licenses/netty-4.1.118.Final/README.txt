Netty 4.1.118.Final licensing provenance

This directory reproduces unmodified upstream Netty source-distribution licensing
and attribution documents. The plugin embeds these Netty runtime artifacts:
netty-common, netty-buffer, netty-transport, netty-resolver, netty-codec,
netty-handler, netty-codec-http, netty-transport-native-unix-common,
netty-handler-proxy, netty-codec-socks.

The native-unix-common artifact supplies Java helper classes; no native transport
classifier is added. Upstream descriptions of optional/native components do not
imply those binaries are included. No extra epoll, kqueue, native TLS, DNS, HTTP/2
or QUIC artifacts are added. The Sink uses JDK TLS and Java NIO and does not
negotiate WebSocket extensions. In particular, upstream licensing text describing
optional native/cryptographic components is attribution provenance, not an added
executable dependency. The license/ files preserve the original NOTICE links.

Source: https://github.com/netty/netty/tree/netty-4.1.118.Final
