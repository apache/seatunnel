---
sidebar_position: 10
---

# TCP NetWork

如果您的环境中多播不是首选的发现方式，那么您可以将 SeaTunnel 引擎配置为一个完整的 TCP/IP 集群。当您通过 TCP/IP 配置 SeaTunnel 引擎以发现成员时，您必须将所有或一部分成员的主机名和/或 IP 地址列为集群成员。您不必列出所有这些集群成员，但在新成员加入时，至少有一个列出的成员必须是活跃的。

要配置您的 Hazelcast 作为一个完整的 TCP/IP 集群，请设置以下配置元素。有关 TCP/IP 发现配置元素的完整描述，请参见 tcp-ip 元素部分。

- 将 tcp-ip 元素的 enabled 属性设置为 true。
- 在 tcp-ip 元素内提供您的成员元素。

以下是一个示例声明性配置。

```yaml
hazelcast:
  network:
    join:
      tcp-ip:
        enabled: true
        member-list:
          - machine1
          - machine2
          - machine3:5799
          - 192.168.1.0-7
          - 192.168.1.21
```

如上所示，您可以为成员元素提供 IP 地址或主机名。您还可以提供一个 IP 地址范围，例如 `192.168.1.0-7`.

除了像上面展示的那样逐行提供成员外，您还可以选择使用 members 元素并写入逗号分隔的 IP 地址，如下所示。

`<members>192.168.1.0-7,192.168.1.21</members>`

如果您没有为成员提供端口，Hazelcast 会自动尝试端口 `5701`, `5702` 等。
