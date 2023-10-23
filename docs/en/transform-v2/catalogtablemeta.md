# CatalogTableMeta

> CatalogTableMeta transform plugin

## Description

Used to modify or add related information to the CatalogTable, which currently contains the following information

> 1、table name \
> 2、catalog name \
> 3、options \
> 4、table comment \
> 5、partitionKeys \
> 6、primaryKey \
> 7、constraintKeys

## Options

|      name      |        type        | required | default value |
|----------------|--------------------|----------|---------------|
| table          | String             | no       |               |
| catalog_name   | String             | no       |               |
| options        | Map<String,String> | no       |               |
| comment        | String             | no       |               |
| partition_keys | List<String>       | no       |               |
| primaryKey     | Object             | no       |               |
| constraintKeys | List<Object>       | no       |               |

### table [String]

New table name, which is used to change the table name

### catalog_name [String]

New catalog name, which is used to change the catalog name

### options [Map<String,String>]

Table options information, which is used to modify or add the table options information

### comment [String]

Table comment information, which is used to modify or add the table comment information

### partition_keys [List<String>]

Table partitionKeys information, which is used to modify or add the table partitionKeys information

### primaryKey [Object]

Table primaryKey information, which is used to modify or add the table primaryKey information

### constraintKeys [List<Object>]

Table constraintKeys information, which is used to modify or add the table constraintKeys information

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

## Example

CatalogTableMeta transform modifies only the relevant information at the table level and does not modify the column information or data in the table. If you need to modify the table related information can be as follows:

```
transform {
  CatalogTableMeta{
    source_table_name = "fake1"
    result_table_name = "fake2"

    # new Table name
    table = "test.public.table1"
    # 分区信息
    partition_keys = ["id"]
    # 表注释
    comment = "test"
    catalog_name = "mysql"
    options {
       "connector" = "jdbc"
       "url" = "cccc"
    }
    primaryKey {
      name = "primary key"
      columnNames = [name]
    }
    constraintKeys = [
       {
          constraintName = "unique_name"
          constraintType = UNIQUE_KEY
          constraintColumns = [
             {
                 columnName = "id"
                 sortType = DESC
             }
          ]
       }
    ]
  }
}
```

If it's a multi-table scenario, then the configuration example is as follows:

```
transform {
  CatalogTableMeta{
    source_table_name = "fake1"
    result_table_name = "fake2"
    table_transform = [
         {
            tablePath = "test.table1"
            # new Table name
            table = "test.public.table1"
            partition_keys = ["id"]
            comment = "test"
            catalog_name = "mysql"
            options {
               "connector" = "jdbc"
               "url" = "cccc"
            }
         }
         ,
          {
             tablePath = "test.table2"
             primaryKey {
               name = "primary key"
               columnNames = [name]
             }
             constraintKeys = [
                {
                   constraintName = "unique_name"
                   constraintType = UNIQUE_KEY
                   constraintColumns = [
                      {
                          columnName = "id"
                          sortType = DESC
                      }
                   ]
                }
            ]
          }
         ]
  }
}


```

## Changelog

### new version

- Add CatalogTableMeta Transform Plugin

