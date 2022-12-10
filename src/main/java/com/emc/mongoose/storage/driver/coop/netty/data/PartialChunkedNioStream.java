package com.emc.mongoose.storage.driver.coop.netty.data;

import static com.emc.mongoose.base.storage.driver.StorageDriver.BUFF_SIZE_MAX;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

/**
 * Modified version of io.netty.handler.stream.ChunkedNioStream with changes from SeekableByteChannelChunkedNioStream.
 * Ensures that the final call to readChunk() writes only a partial chunk to the given byte channel.
 */
public class PartialChunkedNioStream implements ChunkedInput<ByteBuf> {
	private final ReadableByteChannel in;
	private final int chunkSize;
	private final int sizeToTransfer;
	private final ByteBuffer byteBuffer;

	private long offset = 0;

	public PartialChunkedNioStream(SeekableByteChannel sbc) throws IOException {
		this(sbc, (int) sbc.size());
	}

	private PartialChunkedNioStream(ReadableByteChannel in, int sizeToTransfer) {
		this.in = in;
		this.chunkSize = (sizeToTransfer > BUFF_SIZE_MAX ? BUFF_SIZE_MAX : sizeToTransfer);
		this.sizeToTransfer = sizeToTransfer;
		this.byteBuffer = ByteBuffer.allocate(chunkSize);
	}

	@Override
	public final boolean isEndOfInput() {
		// Offset may exceed size when size is not a multiple of chunk size
		return offset >= sizeToTransfer;
	}

	@Override
	public void close() throws Exception {
		in.close();
	}

	@Override
	@Deprecated
	public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
		throw new UnsupportedOperationException("readChunk(ChannelHandlerContext) is not supported");
	}

	@Override
	public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
		if (isEndOfInput()) {
			return null;
		}

		int readBytes = byteBuffer.position();

		// Read a whole chunk
		for (;;) {
			int localReadBytes = in.read(byteBuffer);
			if (localReadBytes < 0) {
				break;
			}

			readBytes += localReadBytes;
			offset += localReadBytes;

			if (readBytes == chunkSize) {
				break;
			}
		}

		byteBuffer.flip();
		boolean release = true;

		// Size may not be a multiple of the chunk size
		// If offset exceeds size, the length should be adjusted
		int length = (offset > sizeToTransfer ? sizeToTransfer - chunkSize : byteBuffer.remaining());

		ByteBuf buffer = allocator.buffer(length);

		// If needed, re-position the byte buffer for the length
		// This should be the last call to readChunk()
		if (length < chunkSize) {
			byteBuffer.position(chunkSize - length);
		}

		// Write the chunk
		try {
			buffer.writeBytes(byteBuffer);
			byteBuffer.clear();
			release = false;
			return buffer;
		} finally {
			if (release) {
				buffer.release();
			}
		}
	}

	@Override
	public long length() {
		return sizeToTransfer;
	}

	@Override
	public long progress() {
		return offset;
	}
}
