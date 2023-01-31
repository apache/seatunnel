# Deduplicate

> Deduplicate transform plugin

## Description

Deduplicate rows by specified fields.

## Options

| name                 | type  | required | default value |
|----------------------| ----- | -------- |---------------|
| duplicate_fields     | array | yes      |               |

### duplicate_fields [array]

Duplicate rows by the field of array

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

## Example

The data read from source is a table like this:

| id | name     | age | 
|----|----------|-----|
| 1  | Joy Ding | 20  | 
| 5  | Joy Ding | 20  | 
| 2  | Kin Dom  | 14  | 
| 9  | Kin Dom  | 14  |

The source table data must sort by duplicate fields. For example, JDBC source:
```
source {
  Jdbc {
      url = "jdbc:mysql://localhost:3306/foo"
      driver = "com.mysql.cj.jdbc.Driver"
      connection_check_timeout_sec = 100
      user = "root"
      password = "******"
      query = "select id, name, age from user order by name"
      result_table_name = "user"
  }
}
```

```
transform {
  Duplicate {
    source_table_name = "user"
    result_table_name = "user1"
    duplicate_fields = ["name"]
  }
}
```

Then the data in result table `user1` will like this

| id | name     | age | 
|----|----------|-----|
| 1  | Joy Ding | 20  | 
| 2  | Kin Dom  | 14  | 

The rows of id 5 and id 9 are deduplicated, and the job result statistic information like this:

```
***********************************************
Start Time                : 0000-00-00 19:25:53
End Time                  : 0000-00-00 19:25:55
Total Time(s)             :                   2
Total Read Count          :                   4
Total Write Count         :                   2
Total Failed Count        :                   2
***********************************************
```


## Changelog

### new version

- Add Deduplicate Transform Connector