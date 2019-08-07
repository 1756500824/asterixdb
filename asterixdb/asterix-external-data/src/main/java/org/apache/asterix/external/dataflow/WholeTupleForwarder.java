package org.apache.asterix.external.dataflow;

import org.apache.asterix.external.api.Forwarder;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameWholeTupleAppender;

public class WholeTupleForwarder implements Forwarder {

    private final FrameWholeTupleAppender appender;
    private final IFrame frame;
    private final IFrameWriter writer;

    public WholeTupleForwarder(IHyracksTaskContext ctx, IFrameWriter writer) throws HyracksDataException {
        this.frame = new VSizeFrame(ctx);
        this.writer = writer;
        this.appender = new FrameWholeTupleAppender(frame);
    }

    public void addTuple(ArrayTupleBuilder tb) throws HyracksDataException {
        DataflowUtils.addWholeTupleToFrame(appender, tb, writer);
    }

    public void flush() throws HyracksDataException {
        appender.flush(writer);
    }

    public void complete() throws HyracksDataException {
        appender.write(writer, false);
        // TODO
    }
}
