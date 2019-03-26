package com.z.flinkStreamOptimizatiion.rpc.common;


import com.google.common.collect.Maps;

import java.util.Map;

public class MessageRegistry {
    private Map<String, Class<?>> clazzes = Maps.newHashMap();
    //type是命令字，clazz是服务端返回数据的类型
    public void register(String type, Class<?> clazz) {
        clazzes.put(type, clazz);
    }
    public Class<?> get(String type) {
        return clazzes.get(type);
    }
}
