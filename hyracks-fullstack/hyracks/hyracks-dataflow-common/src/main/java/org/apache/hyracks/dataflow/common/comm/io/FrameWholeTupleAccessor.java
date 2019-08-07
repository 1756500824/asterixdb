package org.apache.hyracks.dataflow.common.comm.io;

import org.apache.hyracks.api.comm.FrameConstants;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameWholeTupleAccessor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.util.IntSerDeUtils;

import java.nio.ByteBuffer;

public class FrameWholeTupleAccessor implements IFrameWholeTupleAccessor {

    private final RecordDescriptor recordDescriptor;
    private int tupleCountOffset;
    private ByteBuffer buffer;
    private int start;
    private StringBuilder stringBuilder;

    public FrameWholeTupleAccessor(RecordDescriptor recordDescriptor) {
        this.recordDescriptor = recordDescriptor;
    }

    @Override
    public void reset(ByteBuffer buffer) {
        reset(buffer, 0, buffer.limit());
    }

    public void reset(ByteBuffer buffer, int start, int length) {
        this.buffer = buffer;
        this.start = start;
        this.tupleCountOffset = start + FrameHelper.getTupleCountOffset(length);
    }

    @Override
    public int getTupleLength(int tupleIndex) {
        return getTupleEndOffset(tupleIndex) - getTupleStartOffset(tupleIndex);
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return start
                + IntSerDeUtils.getInt(buffer.array(), tupleCountOffset - FrameConstants.SIZE_LEN * (tupleIndex + 1));
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        int offset = tupleIndex == 0 ? FrameConstants.TUPLE_START_OFFSET
                : IntSerDeUtils.getInt(buffer.array(), tupleCountOffset - 4 * tupleIndex);
        return start + offset;
    }

    @Override
    public int getTupleCount() {
        return IntSerDeUtils.getInt(buffer.array(), tupleCountOffset);
    }

    @Override
    public ByteBuffer getBuffer() {
        return null;
    }

    public StringBuilder getTuple(int tupleIndex) {
        int from = getTupleStartOffset(tupleIndex);
        int to = getTupleEndOffset(tupleIndex);
        stringBuilder = new StringBuilder();
        byte[] tempByte = buffer.array();
        for (int i = from; i < to; ++i) stringBuilder.append(tempByte[i]);
        return stringBuilder;
    }
}
