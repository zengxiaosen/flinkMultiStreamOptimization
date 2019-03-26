package com.z.flinkStreamOptimizatiion.rpc.demo;

import com.z.flinkStreamOptimizatiion.rpc.common.IMessageHandler;
import com.z.flinkStreamOptimizatiion.rpc.common.MessageOutput;
import com.z.flinkStreamOptimizatiion.rpc.server.RPCServer;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;


//斐波那契和指数计算处理
class FibRequestHandler implements IMessageHandler<Integer> {

    private List<Long> fibs = new ArrayList<>();

    {
        fibs.add(1L); // fib(0) = 1
        fibs.add(1L); // fib(1) = 1
    }

    @Override
    public void handle(ChannelHandlerContext ctx, String requestId, Integer n) {
        for (int i = fibs.size(); i < n + 1; i++) {
            long value = fibs.get(i - 2) + fibs.get(i - 1);
            fibs.add(value);
        }
        //响应输出
        ctx.writeAndFlush(new MessageOutput(requestId, "fib_res", fibs.get(n)));
    }

}

//构建RPC服务器
//RPC服务器要监听指定IP端口，设定IO线程数和业务线程数
//然后注册斐波那契服务输入类，还有响应的计算处理器
public class DemoServer {


    public static void main(String[] args) {
        //RPCServer server = new RPCServer("localhost", 8888, 2, 16);
        RPCServer server = new RPCServer("localhost", 8888, 2, 16);
        server.service("fib", Integer.class, new FibRequestHandler());
        server.start();


    }
}
