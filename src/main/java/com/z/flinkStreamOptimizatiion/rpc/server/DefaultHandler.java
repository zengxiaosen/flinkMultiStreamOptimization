package com.z.flinkStreamOptimizatiion.rpc.server;

import com.z.flinkStreamOptimizatiion.rpc.common.IMessageHandler;
import com.z.flinkStreamOptimizatiion.rpc.common.MessageInput;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultHandler implements IMessageHandler<MessageInput> {

    private final static Logger LOG = LoggerFactory.getLogger(DefaultHandler.class);
    @Override
    public void handle(ChannelHandlerContext ctx, String requestId, MessageInput input) {
        LOG.error("unrecognized message type {} comes", input.getType());
        ctx.close();
    }
}
