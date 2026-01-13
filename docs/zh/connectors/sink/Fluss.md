import ChangeLog from '../changelog/connector-fluss.md';

# Fluss

> Fluss 数据接收器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精准一次](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

## 描述

该接收器用于将数据写入到Fluss中。支持批和流两种模式。

## 依赖
        <dependency>
            <groupId>com.alibaba.fluss</groupId>
            <artifactId>fluss-client</artifactId>
            <version>0.7.0</version>
        </dependency>


## 接收器选项

| 名称                | 类型     | 是否必须 | 默认值 | Description                                                                      |
|-------------------|--------|------|-----|----------------------------------------------------------------------------------|
| bootstrap.servers | string | yes  | -   | fluss 集群地址                                                                       |
| database          | string | no   | -   | 指定目标 Fluss 表所在的数据库的名称, 如果没有设置该值，则表名与上游库名相同                                       |
| table             | string | no   | -   | 指定目标 Fluss 表的名称, 如果没有设置该值，则表名与上游表名相同                                             |
| client.config     | Map    | no   | -   | 设置其他客户端配置. 参考  https://fluss.apache.org/docs/engine-flink/options/#other-options |


### database [string]

database选项参数可以填入一任意库名，这个名字最终会被用作目标表的库名，并且支持变量（`${database_name}`，`${schema_name}`）。
替换规则如下：`${schema_name}` 将替换传递给目标端的 SCHEMA 名称，`${database_name}` 将替换传递给目标端的库名。

例如：
1. test_${schema_name}_test
2. sink_sinkdb
3. ss_${database_name}


### table [string]

table选项参数可以填入一任意表名，这个名字最终会被用作目标表的表名，并且支持变量（`${table_name}`，`${schema_name}`）。
替换规则如下：`${schema_name}` 将替换传递给目标端的 SCHEMA 名称，`${table_name}` 将替换传递给目标端的表名。

例如：
1. test_${schema_name}_test
2. sink_sinktable
3. ss_${table_name}

## 数据类型映射

| FLuss数据类型    | SeaTunnel数据类型 |
|--------------|---------------|
| BOOLEAN      | BOOLEAN       |
| TINYINT      | TINYINT       |
| SMALLINT     | SMALLINT      |
| INT          | INT           |
| BIGINT       | BIGINT        |
| FLOAT        | FLOAT         |
| DOUBLE       | DOUBLE        |
| DOUBLE       | DOUBLE        |
| BYTES        | BYTES         |
| DATE         | DATE          |
| TIME         | TIME          |
| TIMESTAMP    | TIMESTAMP     |
| TIMESTAMP_TZ | TIMESTAMP_TZ  |
| STRING       | STRING        |


## 任务示例

### 简单示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  FakeSource {
    parallelism = 1
    tables_configs = [
        {
        row.num = 7
          schema {
            table = "test.table1"
            fields {
        	fbytes = bytes
		    fboolean = boolean
		    fint = int
		    ftinyint = tinyint
		    fsmallint = smallint
		    fbigint = bigint
		    ffloat = float
		    fdouble = double
		    fdecimal = "decimal(30, 8)"
		    fstring = string
		    fdate = date
		    ftime = time
		    ftimestamp = timestamp
		    ftimestamp_ltz = timestamp_tz
		    }
	    }
	    rows = [
      {
        kind = INSERT
        fields = ["bWlJWmo=", true, 1940337748, 73, 17489, 7408919466156976747, 9.434991E37, 3.140411637757371E307, 4029933791018936061944.80602290, "aaaaa", "2025-01-03", "02:30:10", "2025-05-27T21:56:09", "2025-09-28T02:54:08+08:00"]
      }
      {
        kind = INSERT
        fields = ["bWlJWmo=", true, 90650390, 37, 22504, 5851888708829345169, 2.6221706E36, 1.8915341983748786E307, 3093109630614622831876.71725344, "bbbbb", "2025-01-01", "21:22:44", "2025-05-08T05:26:18", "2025-08-04T16:49:45+08:00"]
      }
      {
        kind = INSERT
        fields = ["bWlJWmo=", true, 2146418323, 79, 19821, 6393905306944584839, 2.0462337E38, 1.4868114385836557E308, 5594947262031769994080.35717665, "ccccc", "2025-10-06", "22:10:40", "2025-03-25T01:49:14", "2025-07-03T11:52:06+08:00"]
      }
      {
        kind = DELETE
        fields = ["bWlJWmo=", true, 2146418323, 79, 19821, 6393905306944584839, 2.0462337E38, 1.4868114385836557E308, 5594947262031769994080.35717665, "ccccc", "2025-10-06", "22:10:40", "2025-03-25T01:49:14", "2025-07-03T11:52:06+08:00"]
      }
      {
        kind = INSERT
        fields = ["bWlJWmo=", true, 82794384, 27, 30339, 5826566947079347516, 2.2137477E37, 1.7737681870839753E308, 3984670873242882274814.90739768, "ddddd", "2025-09-13", "10:32:52", "2025-01-27T19:20:51", "2025-11-07T02:38:54+08:00"]
      }
      {
        kind = UPDATE_BEFORE
        fields = ["bWlJWmo=", true, 82794384, 27, 30339, 5826566947079347516, 2.2137477E37, 1.7737681870839753E308, 3984670873242882274814.90739768, "ddddd", "2025-09-13", "10:32:52", "2025-01-27T19:20:51", "2025-11-07T02:38:54+08:00"]
      }
      {
        kind = UPDATE_AFTER
        fields = ["bWlJWmo=", true, 388742243, 89, 15831, 159071788675312856, 7.310445E37, 1.2166972324288247E308, 7994947075691901110245.55960937, "ddddd", "2025-01-04", "15:28:07", "2025-07-18T08:59:49", "2025-09-12T23:46:25+08:00"]
      }
    ]
    }
      ]
}
}

transform {
}

sink {
  Fluss {
    bootstrap.servers="fluss_coordinator_e2e:9123"
    database = "fluss_db_${database_name}"
    table = "fluss_tb_${table_name}"
  }
}
```
### 多表写入

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  FakeSource {
    parallelism = 1
    tables_configs = [
        {
        row.num = 7
          schema {
            table = "test2.table1"
            fields {
        	fbytes = bytes
		    fboolean = boolean
		    fint = int
		    ftinyint = tinyint
		    fsmallint = smallint
		    fbigint = bigint
		    ffloat = float
		    fdouble = double
		    fdecimal = "decimal(30, 8)"
		    fstring = string
		    fdate = date
		    ftime = time
		    ftimestamp = timestamp
		    ftimestamp_ltz = timestamp_tz
		    }
	    }
	    rows = [
      {
        kind = INSERT
        fields = ["bWlJWmo=", true, 1940337748, 73, 17489, 7408919466156976747, 9.434991E37, 3.140411637757371E307, 4029933791018936061944.80602290, "aaaaa", "2025-01-03", "02:30:10", "2025-05-27T21:56:09", "2025-09-28T02:54:08+08:00"]
      }
      {
        kind = INSERT
        fields = ["bWlJWmo=", true, 90650390, 37, 22504, 5851888708829345169, 2.6221706E36, 1.8915341983748786E307, 3093109630614622831876.71725344, "bbbbb", "2025-01-01", "21:22:44", "2025-05-08T05:26:18", "2025-08-04T16:49:45+08:00"]
      }
      {
        kind = INSERT
        fields = ["bWlJWmo=", true, 2146418323, 79, 19821, 6393905306944584839, 2.0462337E38, 1.4868114385836557E308, 5594947262031769994080.35717665, "ccccc", "2025-10-06", "22:10:40", "2025-03-25T01:49:14", "2025-07-03T11:52:06+08:00"]
      }
      {
        kind = DELETE
        fields = ["bWlJWmo=", true, 2146418323, 79, 19821, 6393905306944584839, 2.0462337E38, 1.4868114385836557E308, 5594947262031769994080.35717665, "ccccc", "2025-10-06", "22:10:40", "2025-03-25T01:49:14", "2025-07-03T11:52:06+08:00"]
      }
      {
        kind = INSERT
        fields = ["bWlJWmo=", true, 82794384, 27, 30339, 5826566947079347516, 2.2137477E37, 1.7737681870839753E308, 3984670873242882274814.90739768, "ddddd", "2025-09-13", "10:32:52", "2025-01-27T19:20:51", "2025-11-07T02:38:54+08:00"]
      }
      {
        kind = UPDATE_BEFORE
        fields = ["bWlJWmo=", true, 82794384, 27, 30339, 5826566947079347516, 2.2137477E37, 1.7737681870839753E308, 3984670873242882274814.90739768, "ddddd", "2025-09-13", "10:32:52", "2025-01-27T19:20:51", "2025-11-07T02:38:54+08:00"]
      }
      {
        kind = UPDATE_AFTER
        fields = ["bWlJWmo=", true, 388742243, 89, 15831, 159071788675312856, 7.310445E37, 1.2166972324288247E308, 7994947075691901110245.55960937, "ddddd", "2025-01-04", "15:28:07", "2025-07-18T08:59:49", "2025-09-12T23:46:25+08:00"]
      }
    ]
    },
    {
        row.num = 7
          schema {
            table = "test2.table2"
            fields {
        	fbytes = bytes
		    fboolean = boolean
		    fint = int
		    ftinyint = tinyint
		    fsmallint = smallint
		    fbigint = bigint
		    ffloat = float
		    fdouble = double
		    fdecimal = "decimal(30, 8)"
		    fstring = string
		    fdate = date
		    ftime = time
		    ftimestamp = timestamp
		    ftimestamp_ltz = timestamp_tz
		    }
	    }
	    rows = [
      {
        kind = INSERT
        fields = ["bWlJWmo=", true, 1940337748, 73, 17489, 7408919466156976747, 9.434991E37, 3.140411637757371E307, 4029933791018936061944.80602290, "aaaaa", "2025-01-03", "02:30:10", "2025-05-27T21:56:09", "2025-09-28T02:54:08+08:00"]
      }
      {
        kind = INSERT
        fields = ["bWlJWmo=", true, 90650390, 37, 22504, 5851888708829345169, 2.6221706E36, 1.8915341983748786E307, 3093109630614622831876.71725344, "bbbbb", "2025-01-01", "21:22:44", "2025-05-08T05:26:18", "2025-08-04T16:49:45+08:00"]
      }
      {
        kind = INSERT
        fields = ["bWlJWmo=", true, 2146418323, 79, 19821, 6393905306944584839, 2.0462337E38, 1.4868114385836557E308, 5594947262031769994080.35717665, "ccccc", "2025-10-06", "22:10:40", "2025-03-25T01:49:14", "2025-07-03T11:52:06+08:00"]
      }
      {
        kind = DELETE
        fields = ["bWlJWmo=", true, 2146418323, 79, 19821, 6393905306944584839, 2.0462337E38, 1.4868114385836557E308, 5594947262031769994080.35717665, "ccccc", "2025-10-06", "22:10:40", "2025-03-25T01:49:14", "2025-07-03T11:52:06+08:00"]
      }
      {
        kind = INSERT
        fields = ["bWlJWmo=", true, 82794384, 27, 30339, 5826566947079347516, 2.2137477E37, 1.7737681870839753E308, 3984670873242882274814.90739768, "ddddd", "2025-09-13", "10:32:52", "2025-01-27T19:20:51", "2025-11-07T02:38:54+08:00"]
      }
      {
        kind = UPDATE_BEFORE
        fields = ["bWlJWmo=", true, 82794384, 27, 30339, 5826566947079347516, 2.2137477E37, 1.7737681870839753E308, 3984670873242882274814.90739768, "ddddd", "2025-09-13", "10:32:52", "2025-01-27T19:20:51", "2025-11-07T02:38:54+08:00"]
      }
      {
        kind = UPDATE_AFTER
        fields = ["bWlJWmo=", true, 388742243, 89, 15831, 159071788675312856, 7.310445E37, 1.2166972324288247E308, 7994947075691901110245.55960937, "ddddd", "2025-01-04", "15:28:07", "2025-07-18T08:59:49", "2025-09-12T23:46:25+08:00"]
      }
    ]
    },
    {
        row.num = 7
          schema {
            table = "test3.table3"
            fields {
        	fbytes = bytes
		    fboolean = boolean
		    fint = int
		    ftinyint = tinyint
		    fsmallint = smallint
		    fbigint = bigint
		    ffloat = float
		    fdouble = double
		    fdecimal = "decimal(30, 8)"
		    fstring = string
		    fdate = date
		    ftime = time
		    ftimestamp = timestamp
		    ftimestamp_ltz = timestamp_tz
		    }
	    }
	    rows = [
      {
        kind = INSERT
        fields = ["bWlJWmo=", true, 1940337748, 73, 17489, 7408919466156976747, 9.434991E37, 3.140411637757371E307, 4029933791018936061944.80602290, "aaaaa", "2025-01-03", "02:30:10", "2025-05-27T21:56:09", "2025-09-28T02:54:08+08:00"]
      }
      {
        kind = INSERT
        fields = ["bWlJWmo=", true, 90650390, 37, 22504, 5851888708829345169, 2.6221706E36, 1.8915341983748786E307, 3093109630614622831876.71725344, "bbbbb", "2025-01-01", "21:22:44", "2025-05-08T05:26:18", "2025-08-04T16:49:45+08:00"]
      }
      {
        kind = INSERT
        fields = ["bWlJWmo=", true, 2146418323, 79, 19821, 6393905306944584839, 2.0462337E38, 1.4868114385836557E308, 5594947262031769994080.35717665, "ccccc", "2025-10-06", "22:10:40", "2025-03-25T01:49:14", "2025-07-03T11:52:06+08:00"]
      }
      {
        kind = DELETE
        fields = ["bWlJWmo=", true, 2146418323, 79, 19821, 6393905306944584839, 2.0462337E38, 1.4868114385836557E308, 5594947262031769994080.35717665, "ccccc", "2025-10-06", "22:10:40", "2025-03-25T01:49:14", "2025-07-03T11:52:06+08:00"]
      }
      {
        kind = INSERT
        fields = ["bWlJWmo=", true, 82794384, 27, 30339, 5826566947079347516, 2.2137477E37, 1.7737681870839753E308, 3984670873242882274814.90739768, "ddddd", "2025-09-13", "10:32:52", "2025-01-27T19:20:51", "2025-11-07T02:38:54+08:00"]
      }
      {
        kind = UPDATE_BEFORE
        fields = ["bWlJWmo=", true, 82794384, 27, 30339, 5826566947079347516, 2.2137477E37, 1.7737681870839753E308, 3984670873242882274814.90739768, "ddddd", "2025-09-13", "10:32:52", "2025-01-27T19:20:51", "2025-11-07T02:38:54+08:00"]
      }
      {
        kind = UPDATE_AFTER
        fields = ["bWlJWmo=", true, 388742243, 89, 15831, 159071788675312856, 7.310445E37, 1.2166972324288247E308, 7994947075691901110245.55960937, "ddddd", "2025-01-04", "15:28:07", "2025-07-18T08:59:49", "2025-09-12T23:46:25+08:00"]
      }
    ]
    }
      ]
}
}

transform {
}

sink {
  Fluss {
    bootstrap.servers="fluss_coordinator_e2e:9123"
    database = "fluss_db_${database_name}"
    table = "fluss_tb_${table_name}"
  }
}
```

## 变更日志

<ChangeLog />
