# Replace

> Transform plugin : Replace [Flink]

## 描述
定义一个字符串替换函数，可以使用在Sql插件中进行特定字段中的字符替换

## 配置

|配置项 | 类型 | 必填 | 默认值 |
| ----- | ------ | -------- |---------------|
| pattern      | string | no       | -             |
| replacement | string  | no      | -             |
| is_regex | boolean  | no      | false         |
| replace_first | boolean  | no      | false         |
| common-options | string | no       | -             |

### pattern [string]

该字符串要匹配的正则表达式或普通字符串字面量，默认为""

### replacement [string]

替换字符串, 默认为""

### is_regex [boolean]

 true ：使用正则表达式匹配进行替换  
 false (default) ：使用普通字符串进行替换

### replace_first [boolean]

true ：替换第一个匹配子串  
false (default) ：替换所有匹配的子串

### 常用配置 [string]

转换插件常用参数，请参考 [Transform Plugin](common-options.mdx) 有关详细信息

## 示例

- 替换第一个正则表达式匹配的字符串  
  Input : Tom's phone number is 123456789 , he's age is 24  
  Output : Tom's phone number is * , he's age is 24

```bash
  # This just created a udf called replace
  Replace {
      pattern="\\d+"
      replacement="*"
      is_regex = "true"
      replace_first = "true"
   }
  # Use the replace function (confirm that the fake table exists)
  sql {
       sql = "select replace(text) as insensitive_text from fake"
  }
```

- 替换所有正则表达式匹配的字符串  
  Input : Tom's phone number is 123456789 , he's age is 24  
  Output : Tom's phone number is * , he's age is *

```bash
  # This just created a udf called replace
  Replace {
      pattern="\\d+"
      replacement="*"
      is_regex = "true"
      #replace_first = "false" 
   }
  # Use the replace function (confirm that the fake table exists)
  sql {
       sql = "select replace(text) as insensitive_text from fake"
  }
```

- 替换字符串中包含"'s"的子串  
  Input : Tom's phone number is 123456789 , he's age is 24  
  Output : Tom is phone number is 123456789 , he is age is 24

```bash
  # This just created a udf called replace
  Replace {
      pattern="'s"
      replacement=" is"
      #is_regex = "false"
      #replace_first = "false"
   }
  # Use the replace function (confirm that the fake table exists)
  sql {
       sql = "select replace(text) as replaced_text from fake"
  }
```