---

sidebar_position: 9
-------------------

In version 2.3.6. SeaTunnel can add tag to worker node, when you submit job you can specify the tag you want to run.  
update the config in `hazelcast.yaml`,

```yaml
hazelcast:
  cluster-name: seatunnel
  network:
    rest-api:
      enabled: true
      endpoint-groups:
        CLUSTER_WRITE:
          enabled: true
        DATA:
          enabled: true
    join:
      tcp-ip:
        enabled: true
        member-list:
          - localhost
    port:
      auto-increment: false
      port: 5801
  properties:
    hazelcast.invocation.max.retry.count: 20
    hazelcast.tcp.join.port.try.count: 30
    hazelcast.logging.type: log4j2
    hazelcast.operation.generic.thread.count: 50
  member-attributes:
    group:
      type: string
      value: platform
    team:
      type: string
      value: team1
```

In this config, we specify the tag by `member-attributes`, the node has `group=platform, team=team1` tags.
Then, when we use this job config to submit job, we can assign the task to this node.

```hacon
env {
  parallelism = 1
  job.mode = "BATCH"
  tag {
    group = "platform"
    team = "team1"
  }
}
source {
  FakeSource {
    result_table_name = "fake"
    parallelism = 1
    schema = {
      fields {
        name = "string"
      }
    }
  }
}
transform {
}
sink {
  console {
    source_table_name="fake"
  }
}
```

**Notice:**
- If not set this tag in config, it will choose the node in all active nodes.
- In you input a not exist tag, like `group=platform, team=team2`, you will get `NoEnoughResourceException` exception.
- if you special multiple tag, it needs all tag exist and value match,  you can add multiple tags to node, but only use few tag to filter node.
like only use `group=platform`

![img.png](resource_tag.png)

