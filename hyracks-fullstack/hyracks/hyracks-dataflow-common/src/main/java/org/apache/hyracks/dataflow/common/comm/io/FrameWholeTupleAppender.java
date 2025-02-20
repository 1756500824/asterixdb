/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.dataflow.common.comm.io;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.IntSerDeUtils;

public class FrameWholeTupleAppender implements IFrameAppender {

    protected IFrame frame;
    protected byte[] array; // cached the getBuffer().array to speed up byte array access a little

    protected int tupleCount;
    protected int tupleDataEndOffset;

    public FrameWholeTupleAppender() {
    }

    public FrameWholeTupleAppender(IFrame frame) throws HyracksDataException {
        reset(frame, true);
    }

    public FrameWholeTupleAppender(IFrame frame, boolean clear) throws HyracksDataException {
        reset(frame, clear);
    }

    @Override
    public void reset(IFrame frame, boolean clear) throws HyracksDataException {
        this.frame = frame;
        if (clear) {
            this.frame.reset();
        }
        reset(getBuffer(), clear);
    }

    protected boolean hasEnoughSpace(int tupleLength) {
        return tupleDataEndOffset + FrameHelper.calcRequiredSpace(0, tupleLength)
                + tupleCount * FrameConstants.SIZE_LEN <= FrameHelper.getTupleCountOffset(frame.getFrameSize());
    }

    protected void reset(ByteBuffer buffer, boolean clear) {
        array = buffer.array();
        if (clear) {
            IntSerDeUtils.putInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()), 0);
            tupleCount = 0;
            tupleDataEndOffset = FrameConstants.TUPLE_START_OFFSET;
        } else {
            tupleCount = IntSerDeUtils.getInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize()));
            tupleDataEndOffset = tupleCount == 0 ? FrameConstants.TUPLE_START_OFFSET
                    : IntSerDeUtils.getInt(array, FrameHelper.getTupleCountOffset(frame.getFrameSize())
                            - tupleCount * FrameConstants.SIZE_LEN);
        }
    }

    @Override
    public int getTupleCount() {
        return tupleCount;
    }

    @Override
    public ByteBuffer getBuffer() {
        return frame.getBuffer();
    }

    @Override
    public void write(IFrameWriter outWriter, boolean clearFrame) throws HyracksDataException {
        getBuffer().clear();
        outWriter.nextFrame(getBuffer());
        if (clearFrame) {
            frame.reset();
            reset(getBuffer(), true);
        }
    }

    protected boolean canHoldNewTuple(int dataLength) throws HyracksDataException {
        if (hasEnoughSpace(dataLength)) {
            return true;
        }
        if (tupleCount == 0) {
            frame.ensureFrameSize(FrameHelper.calcAlignedFrameSizeToStore(0, dataLength, frame.getMinSize()));
            reset(frame.getBuffer(), true);
            return true;
        }
        return false;
    }

    @Override
    public void flush(IFrameWriter writer) throws HyracksDataException {
        if (tupleCount > 0) {
            write(writer, true);
        }
        writer.flush();
    }

    //    public void flush(IFrameWriter writer, ITracer tracer, String name, long traceCategory, String args)
    //            throws HyracksDataException {
    //        final long tid = tracer.durationB(name, traceCategory, args);
    //        try {
    //            flush(writer);
    //        } finally {
    //            tracer.durationE(tid, traceCategory, args);
    //        }
    //    }
    public boolean append(byte[] bytes, int offset, int length) throws HyracksDataException {
        if (canHoldNewTuple(length)) {
            System.arraycopy(bytes, offset, array, tupleDataEndOffset, length);
            tupleDataEndOffset += length;
            IntSerDeUtils.putInt(getBuffer().array(),
                    FrameHelper.getTupleCountOffset(frame.getFrameSize()) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            IntSerDeUtils.putInt(getBuffer().array(), FrameHelper.getTupleCountOffset(frame.getFrameSize()),
                    tupleCount);
            return true;
        }
        return false;
    }
}
