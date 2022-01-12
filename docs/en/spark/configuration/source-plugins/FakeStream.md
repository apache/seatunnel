# Source plugin : FakeStream [Spark]

## Description

`FakeStream` is mainly used to conveniently generate user-specified data, which is used as input for functional verification, testing, and performance testing of seatunnel.

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| content        | array  | no       | -             |
| rate           | number | yes      | -             |
| common-options | string | yes      | -             |

### content [array]

List of test data strings

### rate [number]

Number of test cases generated per second

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](./source-plugin.md) for details

## Examples

```bash
fakeStream {
    content = ['name=ricky&age=23', 'name=gary&age=28']
    rate = 5
}
```

The generated data is as follows, randomly extract the string from the `content` list

```bash
+-----------------+
|raw_message      |
+-----------------+
|name=gary&age=28 |
|name=ricky&age=23|
+-----------------+
```
