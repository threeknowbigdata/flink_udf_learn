package org.threeknowbigdata.udf;

import com.alibaba.fastjson.JSONObject;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class GetJsonObjectWithCache extends ScalarFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetJsonObject.class);

    private static final Cache<String, JSONObject> JsonCache;

    static {
        JsonCache = Caffeine.newBuilder().expireAfterAccess(30, TimeUnit.MINUTES).maximumSize(1000).build();
        LOGGER.info("using json cache for get json object");
    }

    public String eval(String json, String key) {
        if (StringUtils.isNotBlank(json)) {
            try {
                JSONObject jsonObject = JsonCache.get(json, new Function<String, JSONObject>() {
                    @Override
                    public JSONObject apply(String s) {
                        return JSONObject.parseObject(json);
                    }
                });
                Object obj = jsonObject.get(key);
                if (obj != null) {
                    return obj.toString();
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


