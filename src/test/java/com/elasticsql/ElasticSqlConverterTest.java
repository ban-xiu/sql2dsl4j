package com.elasticsql;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ElasticSqlConverterTest {

    /**
     * 测试简单的SELECT语句
     */
    @Test
    public void testSimpleSelect() throws Exception {
        String sql = "SELECT id, name FROM users WHERE age > 18";
        String dsl = ElasticSqlConverter.convert(sql);
        String table = ElasticSqlConverter.getTableName(sql);
        System.out.println("Simple SELECT DSL: " + dsl);
        System.out.println("Table name: " + table);
        assertNotNull(dsl);
        assertEquals("users", table);
    }

    /**
     * 测试包含AND条件的SELECT语句
     */
    @Test
    public void testSelectWithAndCondition() throws Exception {
        String sql = "SELECT id, name FROM users WHERE age > 18 AND status = 'active'";
        String dsl = ElasticSqlConverter.convert(sql);
        String table = ElasticSqlConverter.getTableName(sql);
        System.out.println("SELECT with AND DSL: " + dsl);
        System.out.println("Table name: " + table);
        assertNotNull(dsl);
        assertEquals("users", table);
        // 验证生成的DSL包含AND条件
        assertTrue(dsl.contains("must"));
    }

    /**
     * 测试包含OR条件的SELECT语句
     */
    @Test
    public void testSelectWithOrCondition() throws Exception {
        String sql = "SELECT id, name FROM users WHERE age > 18 OR status = 'vip'";
        String dsl = ElasticSqlConverter.convert(sql);
        String table = ElasticSqlConverter.getTableName(sql);
        System.out.println("SELECT with OR DSL: " + dsl);
        System.out.println("Table name: " + table);
        assertNotNull(dsl);
        assertEquals("users", table);
        // 验证生成的DSL包含OR条件
        assertTrue(dsl.contains("should"));
        assertTrue(dsl.contains("minimum_should_match"));
    }

    /**
     * 测试包含复杂AND和OR组合条件的SELECT语句
     */
    @Test
    public void testSelectWithComplexConditions() throws Exception {
        String sql = "SELECT id, name FROM users WHERE (age > 18 AND status = 'active') OR (department = 'IT' AND salary > 5000)";
        String dsl = ElasticSqlConverter.convert(sql);
        String table = ElasticSqlConverter.getTableName(sql);
        System.out.println("SELECT with complex conditions DSL: " + dsl);
        System.out.println("Table name: " + table);
        assertNotNull(dsl);
        assertEquals("users", table);
        // 验证生成的DSL包含复杂的条件组合
        assertTrue(dsl.contains("must"));
        assertTrue(dsl.contains("should"));
    }

    @Test
    public void testSelectWithWhere() throws Exception {
        String sql = "SELECT * FROM users WHERE age > 18";
        String dsl = ElasticSqlConverter.convert(sql);
        System.out.println("SELECT with WHERE DSL: " + dsl);
        assertNotNull(dsl);
        assertTrue(dsl.contains("age"));
        assertTrue(dsl.contains("18"));
    }

    @Test
    public void testSelectWithAggregation() throws Exception {
        String sql = "SELECT COUNT(*) FROM users";
        String dsl = ElasticSqlConverter.convert(sql);
        System.out.println("SELECT with aggregation DSL: " + dsl);
        assertNotNull(dsl);
        assertTrue(dsl.contains("value_count"));
        assertTrue(dsl.contains("_index"));
    }

    @Test
    public void testSelectWithLimitAndOrderBy() throws Exception {
        String sql = "SELECT * FROM users ORDER BY name ASC LIMIT 10 OFFSET 5";
        String dsl = ElasticSqlConverter.convert(sql);
        System.out.println("SELECT with LIMIT and ORDER BY DSL: " + dsl);
        assertNotNull(dsl);
        assertTrue(dsl.contains("size") && dsl.contains("10"));
        assertTrue(dsl.contains("from") && dsl.contains("5"));
        assertTrue(dsl.contains("name") && dsl.contains("asc"));
    }

    @Test
    public void testConvertPretty() throws Exception {
        String sql = "SELECT * FROM users WHERE age > 18";
        String prettyDsl = ElasticSqlConverter.convertPretty(sql);
        System.out.println("Pretty DSL: " + prettyDsl);
        assertNotNull(prettyDsl);
        assertTrue(prettyDsl.contains("\n")); // 确保有换行符
    }

    @Test
    public void testUnsupportedStatement() {
        String sql = "UPDATE users SET name = 'John' WHERE id = 1";
        assertThrows(Exception.class, () -> {
            ElasticSqlConverter.convert(sql);
        });
    }

    /**
     * 测试包含GROUP BY子句的SELECT语句
     */
    @Test
    public void testSelectWithGroupBy() throws Exception {
        String sql = "SELECT department, COUNT(*) as employee_count FROM users GROUP BY department";
        String dsl = ElasticSqlConverter.convert(sql);
        System.out.println("SELECT with GROUP BY DSL: " + dsl);
        assertNotNull(dsl);
        // 验证生成的DSL包含terms聚合（GROUP BY）和value_count聚合（COUNT）
        assertTrue(dsl.contains("terms"));
        assertTrue(dsl.contains("value_count"));
        assertTrue(dsl.contains("department"));
        assertTrue(dsl.contains("employee_count"));
        // 验证查询size为0
        assertTrue(dsl.contains("size"));
        assertTrue(dsl.contains("0"));
    }

    /**
     * 测试包含多个GROUP BY字段的SELECT语句
     */
    @Test
    public void testSelectWithMultipleGroupBy() throws Exception {
        String sql = "SELECT department, status, SUM(salary) as total_salary FROM users GROUP BY department, status";
        String dsl = ElasticSqlConverter.convert(sql);
        System.out.println("SELECT with multiple GROUP BY DSL: " + dsl);
        assertNotNull(dsl);
        // 验证生成的DSL包含嵌套的terms聚合
        assertTrue(dsl.contains("terms"));
        assertTrue(dsl.contains("sum"));
        assertTrue(dsl.contains("department"));
        assertTrue(dsl.contains("status"));
        assertTrue(dsl.contains("total_salary"));
        // 验证查询size为0
        assertTrue(dsl.contains("size"));
        assertTrue(dsl.contains("0"));
    }

    /**
     * 测试只有GROUP BY没有聚合函数的SELECT语句
     */
    @Test
    public void testSelectWithOnlyGroupBy() throws Exception {
        String sql = "SELECT department FROM users GROUP BY department";
        String dsl = ElasticSqlConverter.convert(sql);
        System.out.println("SELECT with only GROUP BY DSL: " + dsl);
        assertNotNull(dsl);
        // 验证生成的DSL包含terms聚合，且默认添加了count聚合
        assertTrue(dsl.contains("terms"));
        assertTrue(dsl.contains("value_count"));
        assertTrue(dsl.contains("department"));
        // 验证查询size为0
        assertTrue(dsl.contains("size"));
        assertTrue(dsl.contains("0"));
    }
}