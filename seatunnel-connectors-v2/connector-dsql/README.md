# AWS DSQL Sink Connector

SeaTunnel connector for writing data to Amazon Aurora DSQL clusters with support for real-time CDC synchronization and multi-table operations.

## Features

- **Real-time CDC Support**: Supports INSERT/UPDATE/DELETE operations from CDC sources
- **Multi-table Synchronization**: Sync multiple tables with custom name mapping (**SeaTunnel Engine only**)
- **Batch Processing**: Configurable batch size for optimal performance
- **Auto Table Creation**: Automatically creates tables based on source schema
- **AWS Authentication**: Multiple authentication methods (AccessKey, Profile, Default)
- **Connection Pooling**: Built-in PostgreSQL connection pooling
- **Retry Mechanism**: Automatic retry with exponential backoff

## Quick Start

### 1. Add Dependency

Add to your `pom.xml`:
```xml
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>connector-dsql</artifactId>
    <version>${seatunnel.version}</version>
</dependency>
```

### 2. Basic Configuration

```hocon
sink {
  DSQL {
    cluster_endpoint = "your-cluster-id.dsql.us-east-1.on.aws"
    database_name = "postgres"
    table_name = "users"
    aws_region = "us-east-1"
    profile_name = "default"
    create_table_if_not_exists = true
    batch_size = 1000
  }
}
```

## Configuration Options

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `cluster_endpoint` | String | DSQL cluster endpoint |
| `database_name` | String | Target database name |
| `aws_region` | String | AWS region |

### Authentication (Choose One)

**Option 1: AWS Profile**
```hocon
profile_name = "your-profile"
```

**Option 2: Access Keys**
```hocon
access_key_id = "AKIAIOSFODNN7EXAMPLE"
secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

### Optional Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `table_name` | - | Single table name (required if not multi-table) |
| `enable_multi_table` | false | Enable multi-table mode |
| `table_mapping` | - | Source to target table mapping |
| `primary_keys` | - | Primary key columns for UPSERT operations |
| `batch_size` | 1000 | Batch size for bulk operations |
| `create_table_if_not_exists` | false | Auto-create tables |
| `max_retries` | 3 | Maximum retry attempts |
| `connection_timeout_ms` | 60000 | Connection timeout |

## Usage Examples

### Single Table Sync

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
}

source {
  MySQL-CDC {
    username = "root"
    password = "password"
    table-names = ["test.users"]
    base-url = "jdbc:mysql://localhost:3306/test"
    startup.mode = "initial"
  }
}

sink {
  DSQL {
    cluster_endpoint = "cluster-id.dsql.us-east-1.on.aws"
    database_name = "postgres"
    table_name = "users"
    aws_region = "us-east-1"
    profile_name = "default"
    primary_keys = ["id"]
    create_table_if_not_exists = true
    batch_size = 1000
  }
}
```

### Multi-Table Sync (SeaTunnel Engine Only)

> **Note**: Multi-table synchronization is only supported when using SeaTunnel Engine. This feature is not available with Spark or Flink engines.

```hocon
sink {
  DSQL {
    cluster_endpoint = "cluster-id.dsql.us-east-1.on.aws"
    database_name = "postgres"
    aws_region = "us-east-1"
    profile_name = "default"
    
    # Multi-table configuration
    enable_multi_table = true
    table_mapping = {
      "source_db.users" = "target_users"
      "source_db.orders" = "target_orders"
    }
    
    primary_keys = ["id"]
    create_table_if_not_exists = true
    batch_size = 1000
  }
}
```

### Production Configuration

```hocon
sink {
  DSQL {
    cluster_endpoint = "cluster-id.dsql.us-east-1.on.aws"
    database_name = "postgres"
    table_name = "events"
    aws_region = "us-east-1"
    profile_name = "production"
    
    # Performance tuning
    batch_size = 5000
    max_retries = 5
    retry_delay_ms = 2000
    connection_timeout_ms = 120000
    socket_timeout_ms = 120000
    
    # Table management
    create_table_if_not_exists = true
    primary_keys = ["event_id", "timestamp"]
    

  }
}
```

## Data Type Mapping

| SeaTunnel Type | DSQL Type |
|----------------|-----------|
| STRING/VARCHAR | VARCHAR(255) |
| BOOLEAN | BOOLEAN |
| TINYINT | TINYINT |
| SMALLINT | SMALLINT |
| INT | INTEGER |
| BIGINT | BIGINT |
| FLOAT | REAL |
| DOUBLE | DOUBLE PRECISION |
| DECIMAL | DECIMAL |
| DATE | DATE |
| TIMESTAMP | TIMESTAMP |
| BYTES | BYTEA |

## Prerequisites

### AWS Setup
1. Create DSQL cluster in AWS console
2. Configure IAM permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dsql:*"
      ],
      "Resource": "*"
    }
  ]
}
```

### Network Access
- Ensure connectivity to DSQL cluster endpoint
- Configure VPC/Security Groups if needed

## Troubleshooting

### Common Issues

**Authentication Error**
```
Check AWS credentials and IAM permissions
Verify profile_name exists in ~/.aws/credentials
```

**Connection Timeout**
```
Increase connection_timeout_ms
Check network connectivity to DSQL endpoint
Verify security group rules
```

**Table Creation Failed**
```
Ensure create_table_if_not_exists = true
Check primary_keys configuration
Verify database permissions
```

### Debug Logging

Enable debug logs in your SeaTunnel configuration:
```
logging.level.org.apache.seatunnel.connectors.seatunnel.dsql = DEBUG
```

## Performance Tuning

- **Batch Size**: Increase `batch_size` for higher throughput (1000-5000)
- **Parallelism**: Set appropriate parallelism in job configuration
- **Connection Pool**: Tune connection timeout settings
- **Primary Keys**: Define primary keys for efficient UPSERT operations

## Limitations

- DSQL uses PostgreSQL-compatible protocol
- Maximum batch size depends on DSQL cluster configuration
- Schema changes require manual intervention
- Complex data types are serialized as JSON

## Support

For issues and feature requests, please create an issue in the SeaTunnel repository.
