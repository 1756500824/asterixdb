package org.apache.hyracks.api.comm;

import java.nio.ByteBuffer;

public interface IFrameWholeTupleAccessor {

    int getTupleLength(int tupleIndex);

    int getTupleEndOffset(int tupleIndex);

    int getTupleStartOffset(int tupleIndex);

    int getTupleCount();

    ByteBuffer getBuffer();

    void reset(ByteBuffer buffer);
}
