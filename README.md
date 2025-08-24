# sql2dsl4j

sql2dsl4j是一个将SQL查询语句转换为Elasticsearch DSL的Java库。它提供了简单易用的API，可以将常见的SQL查询转换为Elasticsearch的查询语言。

## 功能特点
- 支持简单SELECT查询转换
- 支持WHERE条件（=、>、<、AND、OR）
- 支持基本聚合函数（COUNT、MIN、MAX、AVG、SUM）
- 支持LIMIT和OFFSET
- 支持ORDER BY
- 提供格式化输出DSL的功能

## 使用示例

```java
// 简单查询
String sql = "SELECT * FROM users";
String dsl = ElasticSqlConverter.convert(sql);
System.out.println(dsl);

// 带WHERE条件的查询
String sqlWithWhere = "SELECT * FROM users WHERE age > 18";
String dslWithWhere = ElasticSqlConverter.convert(sqlWithWhere);

// 带聚合的查询
String sqlWithAgg = "SELECT COUNT(*) FROM users";
String dslWithAgg = ElasticSqlConverter.convert(sqlWithAgg);

// 格式化输出
String prettyDsl = ElasticSqlConverter.convertPretty(sql);
System.out.println(prettyDsl);
```

## 构建和测试

```bash
# 构建项目
mvn clean package

# 运行测试
mvn test
```

## 局限性
- 目前只支持SELECT语句
- WHERE条件处理相对简单
- 不支持复杂的SQL功能如JOIN、子查询等
- GROUP BY子句支持有限

## 不支持
- 不支持更多SQL语句类型（UPDATE、INSERT、DELETE）
- 不支持WHERE条件处理能力
- 不支持GROUP BY和聚合功能
- 不支持JOIN和子查询
