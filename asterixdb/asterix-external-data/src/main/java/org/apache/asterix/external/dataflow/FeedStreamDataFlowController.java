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
package org.apache.asterix.external.dataflow;

import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FeedStreamDataFlowController extends AbstractFeedDataFlowController {

    private final IStreamDataParser dataParser;
    private final IRecordReader recordReader;
    private final AsterixInputStream stream;
    private final boolean canParallel;
    protected long incomingRecordsCount = 0;

    public FeedStreamDataFlowController(IHyracksTaskContext ctx, FeedLogManager feedLogManager,
            IStreamDataParser dataParser, IRecordReader recordReader, AsterixInputStream stream, boolean canParallel) {
        super(ctx, feedLogManager, 1);
        this.dataParser = dataParser;
        this.recordReader = recordReader;
        this.stream = stream;
        this.canParallel = canParallel;
    }

    @Override
    public void start(IFrameWriter writer) throws HyracksDataException {
        try {
            tupleForwarder = new TupleForwarder(ctx, writer);
            if (canParallel) {
                IRawRecord record;
                while (recordReader.hasNext()) {
                    record = recordReader.next();
                    tb.reset();
                    tb.addField(record.getBytes(), 0, record.size());
                    tupleForwarder.addTuple(tb);
                    incomingRecordsCount++;
                }
                tupleForwarder.complete();
            } else {
                while (true) {
                    if (!parseNext()) {
                        break;
                    }
                    tb.addFieldEndOffset();
                    tupleForwarder.addTuple(tb);
                    incomingRecordsCount++;
                }
            }
        } catch (Throwable e) {
            throw HyracksDataException.create(e);
        }
    }

    private boolean parseNext() throws HyracksDataException {
        while (true) {
            try {
                tb.reset();
                return dataParser.parse(tb.getDataOutput());
            } catch (Exception e) {
                if (canParallel) {
                    if (recordReader.handleException(e)) {
                        throw e;
                    }
                } else {
                    if (!handleException(e)) {
                        throw e;
                    }
                }
            }
        }
    }

    @Override
    public boolean stop(long timeout) throws HyracksDataException {
        try {
            if (canParallel) {
                if (recordReader.stop()) {
                    return true;
                }
                recordReader.close();
            } else {
                if (stream.stop()) {
                    return true;
                }
                stream.close();
            }
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
        return false;
    }

    private boolean handleException(Throwable th) {
        boolean handled = true;
        try {
            handled &= stream.handleException(th);
            if (handled) {
                handled &= dataParser.reset(stream);
            }
        } catch (Exception e) {
            th.addSuppressed(e);
            return false;
        }
        return handled;
    }

    @Override
    public String getStats() {
        return "{\"incoming-records-number\": " + incomingRecordsCount + "}";
    }
}
