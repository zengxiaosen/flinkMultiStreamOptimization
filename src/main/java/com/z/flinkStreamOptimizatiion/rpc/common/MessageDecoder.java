package com.z.flinkStreamOptimizatiion.rpc.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

//消息解码器
//使用Netty的ReplayingDecoder实现。简单起见，这里没有使用checkpoint去优化性能了
public class MessageDecoder extends ReplayingDecoder<MessageInput> {

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		String requestId = readStr(in);
		String type = readStr(in);
		String content = readStr(in);
		out.add(new MessageInput(type, requestId, content));
	}

	private String readStr(ByteBuf in) {
		int len = in.readInt();
		if (len < 0 || len > (1 << 20)) {
			throw new DecoderException("string too long len=" + len);
		}
		byte[] bytes = new byte[len];
		in.readBytes(bytes);
		return new String(bytes, Charsets.UTF8);
	}

}
