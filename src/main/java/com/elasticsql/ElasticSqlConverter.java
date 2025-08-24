package com.elasticsql;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

import java.util.HashMap;
import java.util.Map;

public class ElasticSqlConverter {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 将SQL转换为Elasticsearch DSL，并格式化输出
     */
    public static String convertPretty(String sql) throws Exception {
        String dsl = convert(sql);
        Map<String, Object> dslMap = objectMapper.readValue(dsl, HashMap.class);
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(dslMap);
    }

    /**
     * 将SQL转换为Elasticsearch DSL
     */
    public static String convert(String sql) throws Exception {
        try {
            Statement stmt = CCJSqlParserUtil.parse(sql);

            if (stmt instanceof Select) {
                return SelectHandler.handleSelect((Select) stmt);
            } else if (stmt instanceof Update) {
                throw new UnsupportedOperationException("Update statements are not supported yet");
            } else if (stmt instanceof Insert) {
                throw new UnsupportedOperationException("Insert statements are not supported yet");
            } else if (stmt instanceof Delete) {
                throw new UnsupportedOperationException("Delete statements are not supported yet");
            } else {
                throw new UnsupportedOperationException("Unsupported SQL statement type");
            }
        } catch (JSQLParserException e) {
            throw new Exception("Failed to parse SQL: " + e.getMessage(), e);
        }
    }

    /**
     * 从SQL字符串中直接获取表名
     * @param sql SQL查询语句
     * @return 表名
     * @throws Exception 解析异常
     */
    public static String getTableName(String sql) throws Exception {
        try {
            // 解析SQL语句
            Statement stmt = CCJSqlParserUtil.parse(sql);

            if (stmt instanceof Select) {
                Select select = (Select) stmt;
                Select selectBody = select.getSelectBody();

                if (selectBody instanceof PlainSelect) {
                    PlainSelect plainSelect = (PlainSelect) selectBody;
                    FromItem fromItem = plainSelect.getFromItem();
                    return SelectHandler.getTableName(fromItem);
                } else {
                    throw new UnsupportedOperationException("Only simple SELECT statements are supported");
                }
            } else if (stmt instanceof Update) {
                Update update = (Update) stmt;
                return update.getTable().getName();
            } else if (stmt instanceof Insert) {
                Insert insert = (Insert) stmt;
                return insert.getTable().getName();
            } else if (stmt instanceof Delete) {
                Delete delete = (Delete) stmt;
                return delete.getTable().getName();
            } else {
                throw new UnsupportedOperationException("Unsupported SQL statement type");
            }
        } catch (JSQLParserException e) {
            throw new Exception("Failed to parse SQL: " + e.getMessage(), e);
        }
    }

}