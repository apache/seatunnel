# Waterdrop Documentation

---

## Core Concepts

### Event

#### Field Name

A valid field name should not contains `.`, `@` and any other characters that not allowed in ANSI standard SQL 2003 syntax.

Reserved field names includes:

*   `__root__` means top level of the event.
*   `__metadata__` means metadata field for internal use.

#### Metadata

Metadata can be set as usual fields, all the fields in metadata are invisible for output, it's just for internal use.

#### Field Reference

Single level: `a`
Multiple level: `a.b.c`
Top leve (Root) Reference: `__root__`

> [TODO] Notes: this design should be compatible with Spark SQL.

---

## Input

### Kafka

---

## Filters

### JSON

### Split

#### Synopsis

| Setting | Input type | Required | Default value |
| --- | --- | --- | --- |
| delimiter | string | no | " " |
| keys | array | yes | [] |
| source_field | string | yes | "" |
| tag_on_failure | string | no | "_tag" |
| target_field | string | no | "\_\_root\_\_" |


#### Details

*	delimiter

regular expression is supported.

*	keys

if number of parts splited by `delimiter` is larger than number of keys in `keys`, the extra parts in the right side will be ignored. 

*	source_field

if `source_field` does not exists, nothing will be done.

*	target_field

---

## SQL

SQL can be used to filter and aggregate events, the underlying techniques is Spark SQL.

For example, the following sql filters events that response_time between [300, 1200] milliseconds.

```
select * from mytable where response_time >= 300 and response_time <= 1200
```

And this sql count sales for each city:

```
select city, count(sales) from mytable group by city
```

Also, You can combine these two sqls into one sql for both filtering and aggregation:

```
select city, count(*) from mytable where response_time >= 300 and response_time <= 1200 group by city
```

Pipeline multiple sqls:

```
sql {
    query {
        table_name = "mytable1"
        sql = "select * from mytable1 where "
    }
    
    query {
        table_name = ""
    }
}
```

### Query

#### Synopsis

| Setting | Input type | Required | Default value |
| --- | --- | --- | --- |
| table_name | string | no | "mytable" |
| sql | string | yes | - |

> TODO : maybe we can add a `schema` settings for explicitly defining table schema. By now, schema is auto generated.

#### Details

* table_name

Registers a temporary table using the given name, the default value is "mytable". You can use it in `sql`, such as:

```
select * from mytable where http_status >= 500
```

* sql

Executes a SQL query using the given sql string.

---

## Output

### Kafka

## Serializer

### Raw

The default serializer is `raw`. If no serializers configured in input/output, `raw` will be used.

#### Synopsis

| Setting | Input type | Required | Default value |
| --- | --- | --- | --- |
| charset | string | no | "utf-8" |

#### Details

*   charset

Serialize or deserialize using the given charset.

Available charsets are:

> [TODO] list all supported charsets, refer to logstash and these links:

https://docs.oracle.com/javase/7/docs/api/java/nio/charset/Charset.html
http://docs.oracle.com/javase/7/docs/technotes/guides/intl/encoding.doc.html
http://www.iana.org/assignments/character-sets/character-sets.xhtml



### JSON

### Tar.gz

> compressed codec

