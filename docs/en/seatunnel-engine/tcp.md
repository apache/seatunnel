---
sidebar_position: 10
---

# TCP Network

If multicast is not the preferred way of discovery for your environment, then you can configure SeaTunnel Engine to be a full TCP/IP cluster. When you configure SeaTunnel Engine to discover members by TCP/IP, you must list all or a subset of the members' host names and/or IP addresses as cluster members. You do not have to list all of these cluster members, but at least one of the listed members has to be active in the cluster when a new member joins.

To configure your Hazelcast to be a full TCP/IP cluster, set the following configuration elements. See the tcp-ip element section for the full descriptions of the TCP/IP discovery configuration elements.

- Set the enabled attribute of the tcp-ip element to true.
- Provide your member elements within the tcp-ip element.

The following is an example declarative configuration.

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

As shown above, you can provide IP addresses or host names for member elements. You can also give a range of IP addresses, such as `192.168.1.0-7`.

Instead of providing members line-by-line as shown above, you also have the option to use the members element and write comma-separated IP addresses, as shown below.

`<members>192.168.1.0-7,192.168.1.21</members>`

If you do not provide ports for the members, Hazelcast automatically tries the ports `5701`, `5702` and so on.
