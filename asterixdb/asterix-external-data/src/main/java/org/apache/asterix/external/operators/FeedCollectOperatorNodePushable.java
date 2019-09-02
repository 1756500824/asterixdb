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
package org.apache.asterix.external.operators;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.feed.dataflow.FeedRuntimeInputHandler;
import org.apache.asterix.external.feed.dataflow.SyncFeedRuntimeInputHandler;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.util.DataflowUtils;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameWholeTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The first operator in a collect job in a feed.
 */
public class FeedCollectOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private static final Logger LOGGER = LogManager.getLogger();
    private final int partition;
    private final FeedConnectionId connectionId;
    private final FeedPolicyAccessor policyAccessor;
    private final ActiveManager activeManager;
    private final IHyracksTaskContext ctx;
    private final boolean canParallel;
    private IRecordDataParser dataParser;
    private FrameWholeTupleAccessor tAccessor;
    private CharArrayRecord record;
    private IFrame frame;
    private FrameTupleAppender appender;
    private ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
    private int parseNum = 0;

    public FeedCollectOperatorNodePushable(IHyracksTaskContext ctx, FeedConnectionId feedConnectionId,
            Map<String, String> feedPolicy, int partition, boolean canParallel) {
        this.ctx = ctx;
        this.partition = partition;
        this.connectionId = feedConnectionId;
        this.policyAccessor = new FeedPolicyAccessor(feedPolicy);
        this.activeManager = (ActiveManager) ((INcApplicationContext) ctx.getJobletContext().getServiceContext()
                .getApplicationContext()).getActiveManager();
        this.canParallel = canParallel;
    }

    @Override
    public void initialize() throws HyracksDataException {
        try {
            ActiveRuntimeId runtimeId =
                    new ActiveRuntimeId(connectionId.getFeedId(), FeedRuntimeType.COLLECT.toString(), partition);
            tAccessor = new FrameWholeTupleAccessor(recordDesc);
            if (policyAccessor.flowControlEnabled()) {
                writer = new FeedRuntimeInputHandler(ctx, connectionId, runtimeId, writer, policyAccessor, tAccessor,
                        activeManager.getFramePool());
            } else {
                writer = new SyncFeedRuntimeInputHandler(ctx, writer, tAccessor);
            }
            frame = new VSizeFrame(ctx);
            appender = new FrameTupleAppender(frame);
            record = new CharArrayRecord();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        if (canParallel) { /* ChangeFeed or RecordWithMeta cannot go into this part, they will execute with single parse*/
            frame.reset();
            appender.reset(frame, true);
            tAccessor.reset(buffer);
            int nTuple = tAccessor.getTupleCount();
            int failedRecordsCount = 0;
            for (int tupleIndex = 0; tupleIndex < nTuple; ++tupleIndex) {
                try {
                    record.set(tAccessor.getTuple(tupleIndex));
                    tb.reset();
                    if (!parseAndForward(record)) {
                        failedRecordsCount++;
                    }
                } catch (IOException e) {
                    LOGGER.log(Level.WARN, "Exception during parsing", e);
                    // TODO deal with the exception
                }
            }
            //            System.out.println(ctx.getJobletContext().getServiceContext().getNodeId());
            //            System.out.printf("This parser parse %d records and %d records failed\n", parseNum, failedRecordsCount);
            //            LOGGER.log(Level.WARN, "{ " + ctx.getJobletContext().getServiceContext().getNodeId() + " has parsed "
            //                    + parseNum + " records and " + failedRecordsCount + " records failed}\n");
            writer.nextFrame(appender.getBuffer());
        } else {
            writer.nextFrame(buffer);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }

    @Override
    public void close() throws HyracksDataException {
        writer.close();
    }

    private boolean parseAndForward(CharArrayRecord record) throws IOException {
        try {
            dataParser.parse(record, tb.getDataOutput());
            parseNum++;
        } catch (Exception e) {
            LOGGER.log(Level.WARN, ExternalDataConstants.ERROR_PARSE_RECORD, e);
            //            feedLogManager.logRecord(record.toString(), ExternalDataConstants.ERROR_PARSE_RECORD);
            return false;
        }
        tb.addFieldEndOffset();
        DataflowUtils.addTupleToFrame(appender, tb, writer);
        return true;
    }

    void setDataParser(IRecordDataParser dataParser) {
        this.dataParser = dataParser;
    }

}
