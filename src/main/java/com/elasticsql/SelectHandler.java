package com.elasticsql;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class SelectHandler {
    /**
     * 处理SELECT语句，转换为Elasticsearch DSL
     */
    public static String handleSelect(Select select) throws Exception {
        Select selectBody = select.getSelectBody();
        if (!(selectBody instanceof PlainSelect)) {
            throw new UnsupportedOperationException("Only simple SELECT statements are supported");
        }

        PlainSelect plainSelect = (PlainSelect) selectBody;

        // 处理WHERE条件
        String queryMapStr = buildQuery(plainSelect.getWhere());

        // 检查是否需要聚合
        boolean aggFlag = checkNeedAgg(plainSelect.getSelectItems());
        String aggStr = "";
        String querySize = "1";

        if (aggFlag || plainSelect.getGroupBy() != null) {
            aggFlag = true;
            querySize = "0";
            aggStr = buildAggs(plainSelect);
        }

        // 处理LIMIT子句
        String queryFrom = "0";
        if (plainSelect.getLimit() != null) {
            if (plainSelect.getOffset() != null) {
                queryFrom = plainSelect.getOffset().getOffset().toString();
            }
            querySize = plainSelect.getLimit().getRowCount().toString();
        }

        // 处理ORDER BY子句
        List<String> orderByArr = new ArrayList<>();
        if (!aggFlag && plainSelect.getOrderByElements() != null) {
            for (OrderByElement orderByElement : plainSelect.getOrderByElements()) {
                String field = orderByElement.getExpression().toString();
                String direction = orderByElement.isAsc() ? "asc" : "desc";
                orderByArr.add(String.format("{\"%s\": \"%s\"}", field, direction));
            }
        }

        // 构建最终的DSL
        JSONObject result = new JSONObject();
        result.put("query", new JSONObject(queryMapStr));
        result.put("from", Integer.parseInt(queryFrom));
        result.put("size", Integer.parseInt(querySize));

        if (!aggStr.isEmpty()) {
            result.put("aggregations", new JSONObject(aggStr));
        }

        if (!orderByArr.isEmpty()) {
            result.put("sort", orderByArr);
        }

        return result.toString();
    }

    /**
     * 构建查询条件
     */
    private static String buildQuery(Expression where) {
        if (where == null) {
            return "{\"bool\": {\"must\": [{\"match_all\": {}}]}}";
        }

        // 处理表达式，返回完整的查询对象
        JSONObject query = processExpression(where);
        return query.toString();
    }

    /**
     * 递归处理表达式，返回完整的查询对象
     */
    private static JSONObject processExpression(Expression expr) {
        if (expr == null) {
            // 返回match_all查询作为默认值
            JSONObject matchAll = new JSONObject();
            matchAll.put("match_all", new JSONObject());
            return matchAll;
        }

        // 处理AND表达式
        if (expr instanceof AndExpression) {
            AndExpression andExpr = (AndExpression) expr;
            Expression leftExpr = andExpr.getLeftExpression();
            Expression rightExpr = andExpr.getRightExpression();

            // 递归处理左右表达式
            JSONObject leftQuery = processExpression(leftExpr);
            JSONObject rightQuery = processExpression(rightExpr);

            // 如果左表达式已经是bool查询且包含must子句，直接将右表达式添加到must子句
            if (leftQuery.has("bool") && leftQuery.getJSONObject("bool").has("must")) {
                JSONObject boolQuery = leftQuery.getJSONObject("bool");
                JSONArray mustArray = boolQuery.getJSONArray("must");
                mustArray.put(rightQuery);
                return leftQuery;
            } else {
                // 创建新的bool查询
                JSONObject boolQuery = new JSONObject();
                JSONArray mustArray = new JSONArray();
                mustArray.put(leftQuery);
                mustArray.put(rightQuery);
                boolQuery.put("must", mustArray);
                
                JSONObject result = new JSONObject();
                result.put("bool", boolQuery);
                return result;
            }
        }
        // 处理OR表达式
        else if (expr instanceof OrExpression) {
            OrExpression orExpr = (OrExpression) expr;
            Expression leftExpr = orExpr.getLeftExpression();
            Expression rightExpr = orExpr.getRightExpression();

            // 递归处理左右表达式
            JSONObject leftQuery = processExpression(leftExpr);
            JSONObject rightQuery = processExpression(rightExpr);

            // 创建新的bool查询，使用should子句
            JSONObject boolQuery = new JSONObject();
            JSONArray shouldArray = new JSONArray();
            shouldArray.put(leftQuery);
            shouldArray.put(rightQuery);
            boolQuery.put("should", shouldArray);
            
            // 默认要求至少一个should条件满足
            boolQuery.put("minimum_should_match", 1);
            
            JSONObject result = new JSONObject();
            result.put("bool", boolQuery);
            return result;
        }
        // 处理Parenthesis（括号）表达式，确保正确处理复杂条件组合
        else if (expr instanceof Parenthesis) {
            Parenthesis parenthesis = (Parenthesis) expr;
            Expression innerExpr = parenthesis.getExpression();
            // 直接返回括号内表达式的处理结果
            return processExpression(innerExpr);
        }
        // 处理比较表达式
        else if (expr instanceof BinaryExpression) {
            BinaryExpression binaryExpr = (BinaryExpression) expr;
            String left = binaryExpr.getLeftExpression().toString();
            String right = binaryExpr.getRightExpression().toString();

            // 移除引号
            if (right.startsWith("'")) {
                right = right.substring(1, right.length() - 1);
            }

            if (expr instanceof EqualsTo) {
                // 处理等于条件
                JSONObject term = new JSONObject();
                JSONObject termQuery = new JSONObject();
                termQuery.put(left, right);
                term.put("term", termQuery);
                return term;
            } else if (expr instanceof GreaterThan) {
                // 处理大于条件
                JSONObject rangeQuery = new JSONObject();
                JSONObject fieldQuery = new JSONObject();
                fieldQuery.put("gt", right);
                rangeQuery.put(left, fieldQuery);

                JSONObject range = new JSONObject();
                range.put("range", rangeQuery);
                return range;
            } else if (expr instanceof GreaterThanEquals) {
                // 处理大于等于条件
                JSONObject rangeQuery = new JSONObject();
                JSONObject fieldQuery = new JSONObject();
                fieldQuery.put("gte", right);
                rangeQuery.put(left, fieldQuery);

                JSONObject range = new JSONObject();
                range.put("range", rangeQuery);
                return range;
            } else if (expr instanceof MinorThan) {
                // 处理小于条件
                JSONObject rangeQuery = new JSONObject();
                JSONObject fieldQuery = new JSONObject();
                fieldQuery.put("lt", right);
                rangeQuery.put(left, fieldQuery);

                JSONObject range = new JSONObject();
                range.put("range", rangeQuery);
                return range;
            } else if (expr instanceof MinorThanEquals) {
                // 处理小于等于条件
                JSONObject rangeQuery = new JSONObject();
                JSONObject fieldQuery = new JSONObject();
                fieldQuery.put("lte", right);
                rangeQuery.put(left, fieldQuery);

                JSONObject range = new JSONObject();
                range.put("range", rangeQuery);
                return range;
            }
        }

        // 默认返回match_all查询
        JSONObject matchAll = new JSONObject();
        matchAll.put("match_all", new JSONObject());
        return matchAll;
    }

    /**
     * 获取表名 - 从FromItem对象中
     */
    public static String getTableName(FromItem fromItem) throws UnsupportedOperationException {
        if (fromItem instanceof Table) {
            return ((Table) fromItem).getName();
        } else {
            throw new UnsupportedOperationException("Only simple FROM clauses are supported");
        }
    }

    /**
     * 检查是否需要聚合
     */
    private static boolean checkNeedAgg(List<SelectItem<?>> selectItems) {
        for (SelectItem item : selectItems) {
            // 使用更通用的方式检查聚合函数
            String itemStr = item.toString().toLowerCase();
            if (itemStr.contains("count") ||
                itemStr.contains("sum") ||
                itemStr.contains("avg") ||
                itemStr.contains("max") ||
                itemStr.contains("min")) {
                return true;
            }
        }
        return false;
    }

    /**
     * 构建聚合查询
     */
    private static String buildAggs(PlainSelect plainSelect) {
        // 简化实现，支持常见聚合函数和GROUP BY
        JSONObject aggregations = new JSONObject();
        JSONObject currentAgg = aggregations;
        List<String> groupByFields = new ArrayList<>();

        // 处理GROUP BY子句
        if (plainSelect.getGroupBy() != null) {
            GroupByElement groupByElement = plainSelect.getGroupBy();
            if (groupByElement.getGroupByExpressions() != null) {
                ExpressionList groupByExpressions = groupByElement.getGroupByExpressions();
                List<Expression> expressions = groupByExpressions.getExpressions();
                for (Expression expr : expressions) {
                    String field = expr.toString();
                    // 移除可能的引号和表名前缀
                    if (field.contains(".")) {
                        field = field.substring(field.lastIndexOf(".") + 1);
                    }
                    if (field.startsWith("'")) {
                        field = field.substring(1, field.length() - 1);
                    }
                    groupByFields.add(field);
                }
            }
        }

        // 构建GROUP BY的嵌套聚合
        for (int i = 0; i < groupByFields.size(); i++) {
            String field = groupByFields.get(i);
            String aggName = "group_by_" + field;
            JSONObject termsAgg = new JSONObject();
            JSONObject terms = new JSONObject();
            terms.put("field", field);
            termsAgg.put("terms", terms);
            
            // 如果是最后一个GROUP BY字段或者没有GROUP BY字段，在这一级添加聚合函数
            if (i == groupByFields.size() - 1) {
                currentAgg.put(aggName, termsAgg);
                currentAgg = termsAgg;
            } else {
                JSONObject nextLevel = new JSONObject();
                termsAgg.put("aggs", nextLevel);
                currentAgg.put(aggName, termsAgg);
                currentAgg = nextLevel;
            }
        }

        // 在最内层聚合中添加SELECT中的聚合函数
        JSONObject metricsAggs = new JSONObject();
        boolean hasMetrics = false;
        
        for (SelectItem item : plainSelect.getSelectItems()) {
            String itemStr = item.toString().toLowerCase();
            String alias = itemStr;
            String funcName = "";
            String field = "";

            // 解析常见聚合函数
            if (itemStr.contains("count")) {
                funcName = "count";
                // 提取字段名
                if (itemStr.contains("count(*)") || itemStr.contains("count(1)")) {
                    field = "*";
                } else {
                    // 简单提取括号中的内容作为字段
                    int startIndex = itemStr.indexOf("(") + 1;
                    int endIndex = itemStr.indexOf(")");
                    if (startIndex > 0 && endIndex > startIndex) {
                        field = itemStr.substring(startIndex, endIndex).trim();
                        // 移除可能的引号和表名前缀
                        if (field.contains(".")) {
                            field = field.substring(field.lastIndexOf(".") + 1);
                        }
                        if (field.startsWith("'")) {
                            field = field.substring(1, field.length() - 1);
                        }
                    }
                }
            } else if (itemStr.contains("sum")) {
                funcName = "sum";
                // 提取字段名
                int startIndex = itemStr.indexOf("(") + 1;
                int endIndex = itemStr.indexOf(")");
                if (startIndex > 0 && endIndex > startIndex) {
                    field = itemStr.substring(startIndex, endIndex).trim();
                    // 移除可能的引号和表名前缀
                    if (field.contains(".")) {
                        field = field.substring(field.lastIndexOf(".") + 1);
                    }
                    if (field.startsWith("'")) {
                        field = field.substring(1, field.length() - 1);
                    }
                }
            } else if (itemStr.contains("avg")) {
                funcName = "avg";
                // 提取字段名
                int startIndex = itemStr.indexOf("(") + 1;
                int endIndex = itemStr.indexOf(")");
                if (startIndex > 0 && endIndex > startIndex) {
                    field = itemStr.substring(startIndex, endIndex).trim();
                    // 移除可能的引号和表名前缀
                    if (field.contains(".")) {
                        field = field.substring(field.lastIndexOf(".") + 1);
                    }
                    if (field.startsWith("'")) {
                        field = field.substring(1, field.length() - 1);
                    }
                }
            } else if (itemStr.contains("max")) {
                funcName = "max";
                // 提取字段名
                int startIndex = itemStr.indexOf("(") + 1;
                int endIndex = itemStr.indexOf(")");
                if (startIndex > 0 && endIndex > startIndex) {
                    field = itemStr.substring(startIndex, endIndex).trim();
                    // 移除可能的引号和表名前缀
                    if (field.contains(".")) {
                        field = field.substring(field.lastIndexOf(".") + 1);
                    }
                    if (field.startsWith("'")) {
                        field = field.substring(1, field.length() - 1);
                    }
                }
            } else if (itemStr.contains("min")) {
                funcName = "min";
                // 提取字段名
                int startIndex = itemStr.indexOf("(") + 1;
                int endIndex = itemStr.indexOf(")");
                if (startIndex > 0 && endIndex > startIndex) {
                    field = itemStr.substring(startIndex, endIndex).trim();
                    // 移除可能的引号和表名前缀
                    if (field.contains(".")) {
                        field = field.substring(field.lastIndexOf(".") + 1);
                    }
                    if (field.startsWith("'")) {
                        field = field.substring(1, field.length() - 1);
                    }
                }
            }

            // 如果识别到聚合函数，则构建聚合查询
            if (!funcName.isEmpty()) {
                hasMetrics = true;
                JSONObject agg = new JSONObject();
                if (funcName.equals("count")) {
                    if (field.equals("*")) {
                        JSONObject valueCount = new JSONObject();
                        valueCount.put("field", "_index");
                        agg.put("value_count", valueCount);
                    } else {
                        JSONObject valueCount = new JSONObject();
                        valueCount.put("field", field);
                        agg.put("value_count", valueCount);
                    }
                } else if (funcName.equals("min") || funcName.equals("max") || funcName.equals("avg") || funcName.equals("sum")) {
                    JSONObject metric = new JSONObject();
                    metric.put("field", field);
                    agg.put(funcName, metric);
                }

                // 尝试提取别名
                if (itemStr.contains(" as ")) {
                    int asIndex = itemStr.indexOf(" as ") + 4;
                    alias = itemStr.substring(asIndex).trim();
                }

                metricsAggs.put(alias, agg);
            }
        }

        // 如果有聚合函数且有GROUP BY字段，将聚合函数添加到最后一级GROUP BY聚合中
        if (hasMetrics && !groupByFields.isEmpty()) {
            if (currentAgg.has("terms")) {
                currentAgg.put("aggs", metricsAggs);
            } else {
                // 如果是中间层级，直接添加聚合函数
                // 替代JSONObject.putAll方法
                for (String key : metricsAggs.keySet()) {
                    currentAgg.put(key, metricsAggs.get(key));
                }
            }
        } else if (hasMetrics) {
            // 如果没有GROUP BY字段，直接添加聚合函数到根级别
            // 替代JSONObject.putAll方法
            for (String key : metricsAggs.keySet()) {
                aggregations.put(key, metricsAggs.get(key));
            }
        } else if (!groupByFields.isEmpty()) {
            // 如果只有GROUP BY没有聚合函数，添加一个默认的count聚合
            JSONObject countAgg = new JSONObject();
            JSONObject valueCount = new JSONObject();
            valueCount.put("field", "_index");
            countAgg.put("value_count", valueCount);
            metricsAggs.put("count", countAgg);
            
            if (currentAgg.has("terms")) {
                currentAgg.put("aggs", metricsAggs);
            } else {
                // 替代JSONObject.putAll方法
                for (String key : metricsAggs.keySet()) {
                    currentAgg.put(key, metricsAggs.get(key));
                }
            }
        }

        return aggregations.toString();
    }
}