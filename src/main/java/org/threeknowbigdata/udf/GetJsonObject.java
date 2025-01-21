package org.threeknowbigdata.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetJsonObject extends ScalarFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetJsonObject.class);

    public String eval(String json, String key) {
        if (StringUtils.isNotBlank(json)) {
            try {
                JSONObject jsonObject = JSONObject.parseObject(json);
                Object value = jsonObject.get(key);
                if (value != null) {
                    //适配如果是json嵌套json,可以直接打成string
                    return value.toString();
                } else {
                    return null;
                }
            } catch (Exception e) {
                LOGGER.warn(String.format("get json object failed, json str:%s, key:%s", json, key), e);
            }
        }
        return null;

    }
}