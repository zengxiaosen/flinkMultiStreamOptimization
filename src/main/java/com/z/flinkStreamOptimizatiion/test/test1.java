package com.z.flinkStreamOptimizatiion.test;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
class A {
    void test(int i) {
        System.out.println("A " + i);
    }
}

class B extends A {
    @Override
    void test(int i) {
        System.out.println("B " + i);
        System.out.println("bbbbb");
    }
}

class C extends B {

}
public class test1 {
    public static void main(String[] args) throws Exception {
        // tm='1908.0', duration='22000.0', count=0.08672727272727272}
        // tm='55041.0', duration='55000.0', count=0.0
        // tm='47097.0', duration='46000.0', count=1.0238478260869566
        // double exp = testExp();
        // testTimestamp();
        // testMap2Json();
        // testpb();
        // testTypeHandler();
        testObj();

    }

    private static void testObj() {
        C c = new C();
        c.test(1);
    }

    private static void testTypeHandler() {
        Map<String, Handler> typeHandler = new HashMap<>();
        typeHandler.put(AllType.a.name(), new AHandler());
        typeHandler.put(AllType.b.name(), new BHandler());
//        typeHandler.put("a", new AHandler());
//        typeHandler.put("b", new BHandler());

        typeHandler.get("b").handleSink(1);
        typeHandler.get("a").handleSink(1);

    }

    public enum AllType {
        a,
        b;
    }

    interface Handler {
        void handleSink(int data);
    }

    static class AHandler implements Handler {
        @Override
        public void handleSink(int data) {
            System.out.println("type: a, value: " + data);
        }
    }

    static class BHandler implements Handler {

        @Override
        public void handleSink(int data) {
            System.out.println("type: b, value: " + data);
        }
    }

    private static void testpb() {
        SliceActVV.userInfo.Builder usrInfo = SliceActVV.userInfo.newBuilder();
        usrInfo.setRtUClick(1L);
        usrInfo.setRtUReveal(2L);
        usrInfo.setRtURpt(0.5);
        SliceActVV.userInfo userInfo2 = usrInfo.build();
        userInfo2 = userInfo2.toBuilder().setRtURpt(0.6).build();
        System.out.println(userInfo2);
    }

    private static void testMap2Json() throws IOException {
        Map<String,Object> map = new HashMap<String,Object>();
        map.put("users", 1);
        map.put("u", 1);
        ObjectMapper objectMapper = new ObjectMapper();
        byte[] ob = objectMapper.writeValueAsBytes(map);
        Map<String, Object> map2 = (HashMap<String, Object>)objectMapper.readValue(ob, Map.class);
        System.out.println(map2);

    }

    private static void testTimestamp() {
        long timestamp = System.currentTimeMillis();
        System.out.println("timestamp: " + timestamp);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        String date = df.format(new Date());// new Date()为获取当前系统时间，也可使用当前时间戳
        System.out.println("date: " + date);
        System.out.println("timestamp date: " + df.format(timestamp));
    }

    private static double testExp() throws Exception{
        double res = 47097.0 / 46000.0;
        System.out.println(res);
        throw new RuntimeException("cao");

    }

}
