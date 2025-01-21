package org.threeknowbigdata.udtf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;


/**
 * @ClassName: RowConvertTableFunction
 * @Description: TODO
 * @Author: liyaozhou
 * @Date: 2022/07/21 14:43:06
 * @Version: V1.0
 **/

@FunctionHint(
        input = @DataTypeHint("STRING"),
        output = @DataTypeHint(
                "ROW<project STRING," +
                        "module STRING," +
                        "userAgeRange STRING," +
                        "userCity STRING," +
                        "itemGender INT," +
                        "itemCnt INT>"
        )
)
public class RowConvertTableFunction extends TableFunction<Row> {

    /**
     * 一行转多行的转换时间
     */
    public static final String ROW_CONVERT_TIME = "rowConvertTime";
    /**
     * item 解析时间
     */
    public static final String ITEM_PARSER_TIME = "itemParserTime";
    /**
     * item 初始条数
     */
    public static final String ITEM_INITIAL_COUNT = "itemInitialCount";
    /**
     * item 聚合后条数
     */
    public static final String ITEM_AGGREGATE_COUNT = "itemAggregateCount";
    /**
     * json 解析异常次数
     */
    public static final String PARSER_EXCEPTION_CNT = "parserExceptionCnt";
    private static final Logger LOG = LoggerFactory.getLogger(RowConvertTableFunction.class);
    private transient Gauge<Long> rowConvertGuage;
    private transient Gauge<Long> itemParserGuage;
    private transient Gauge<Integer> itemInitialCounter;
    private transient Gauge<Integer> itemAggregateCounter;
    private transient Counter parserExceptionCnt;

    private String project;

    private String module;

    private String userAgeRange;

    private String userCity;

    private int itemGender;

    private Map<JSONObject, Integer> itemObject;

    private int parserExceptionCount;

    private int errorCnt;

    private int itemCnt = 1;
    // 用于存储行转换时间
    private long rowConvertTime;
    // 用于存储 item 解析时间
    private long itemParserTime;
    // 用于存储 item 初始条数
    private int itemInitialCount;
    // 用于存储 item 聚合后条数
    private int itemAggregateCount;

    public RowConvertTableFunction() {
        itemObject = new LinkedHashMap<>();
        parserExceptionCount = 0;
        errorCnt = 1;
    }

    /**
     * @param context 创建 Metrics 监控指标
     */
    @Override
    public void open(FunctionContext context) {
        MetricGroup metricGroup = context.getMetricGroup();
        // 创建和注册 Gauge 指标
        rowConvertGuage = metricGroup.gauge(ROW_CONVERT_TIME, () -> rowConvertTime);
        itemParserGuage = metricGroup.gauge(ITEM_PARSER_TIME, () -> itemParserTime);
        itemInitialCounter = metricGroup.gauge(ITEM_INITIAL_COUNT, () -> itemInitialCount);
        itemAggregateCounter = metricGroup.gauge(ITEM_AGGREGATE_COUNT, () -> itemAggregateCount);
        // 创建和注册 Counter 指标
        parserExceptionCnt = metricGroup.counter(PARSER_EXCEPTION_CNT);
    }

    /**
     * @param str 输入是一行字符串，对应的是 json 格式的数据
     */
    public void eval(String str) {
        try {
            long startTime = System.currentTimeMillis();
            long parserJsonStartTime = System.nanoTime();
            // 将数据的 str 字符串转为 json 对象
            JSONObject jsonObject = JSON.parseObject(str);
            // 1 解析最外层 json 数据，获取结果
            JsonObjectParser(jsonObject);
            // 获取 jsonArray 数组
            JSONArray item = jsonObject.getJSONArray("item");
            long parserItemStartTime = System.nanoTime();
            // 监控指标： 获取单个 item 聚合前的数量
            itemInitialCount = item.size();
            // 2 对 item 中相同的对象做聚合操作
            itemObject = itemAggregation(item);
            // 监控指标： 获取单个 item 聚合前的数量
            itemParserTime = System.nanoTime() - parserItemStartTime;
            // 监控指标：获取单个 item 聚合后的数量
            itemAggregateCount = itemObject.size();
            // 3 对解析后的结果进行收集
            collectResult(itemObject);
            // 监控指标：获取整个字符串的转换的时间
            rowConvertTime = System.nanoTime() - parserJsonStartTime;
            // 清除 HashMap 存储记录
            itemObject.clear();
            long executionTime = System.currentTimeMillis() - startTime;
            LOG.info("data conversion takes {} ms", executionTime);
        } catch (Exception e) {
            ++parserExceptionCount;
            parserExceptionCnt.inc();
            if (parserExceptionCount % errorCnt == 0) {
                LOG.error("json parse failed, the number of parse errors is {}", parserExceptionCount, e);
                if (errorCnt!= 100000) {
                    errorCnt *= 10;
                }
            }
        }
    }

    /**
     * @param jsonObject 对获取的 json 数据进行解析
     */
    public void JsonObjectParser(JSONObject jsonObject) {
        project = jsonObject.getString("project");
        module = jsonObject.getString("module");
        userAgeRange = jsonObject.getJSONObject("user").getString("ageRange");
        userCity = jsonObject.getJSONObject("user").getString("city");
    }

    /**
     * @param item : 对输入的 item  Array格式进行解析，并聚合 item 中重复的数据
     * @return itemObject: 返回 Map 集合，key 为 包含的json 对象，value 为聚合后的统计值
     */
    public Map<JSONObject, Integer> itemAggregation(JSONArray item) {
        if (item!= null && item.size() > 0) {
            // 遍历 jsonArray 数组，获取每个对象
            for (int i = 0; i < item.size(); i++) {
                JSONObject itemObj = item.getJSONObject(i);
                // 删除每个对象中的 id 属性
                itemObj.remove("id");
                // 通过 HashMap 对每个对象进行判断，如果集合中包含这个对象，value 值 +1
                if (itemObject.containsKey(itemObj)) {
                    Integer count = itemObject.get(itemObj);
                    itemObject.put(itemObj, ++count);
                } else {
                    itemObject.put(itemObj, itemCnt);
                }
            }
        }
        return itemObject;
    }

    /**
     * @param itemObject
     */
    public void collectResult(Map<JSONObject, Integer> itemObject) {
        Row row = null;
        if (itemObject.size()!= 0) {
            // 遍历 HashMap 集合，
            for (Map.Entry<JSONObject, Integer> entry : itemObject.entrySet()) {
                itemGender = Integer.parseInt(entry.getKey().getString("gender"));
                itemCnt = entry.getValue();
                // 将输出类型封装成 Row 格式返回。
                row = Row.of(project, module, userAgeRange,
                        userCity, itemGender, itemCnt);
                collect(row);
            }
        } else {
            itemGender = 0;
            itemCnt = 1;
            row = Row.of(project, module, userAgeRange,
                    userCity, itemGender, itemCnt);
            collect(row);
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(
                Types.STRING,
                Types.STRING,
                Types.STRING,
                Types.STRING,
                Types.INT,
                Types.INT);
    }
}