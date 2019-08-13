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
package org.apache.asterix.external.parser.controller;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordWithMetadataParser;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class FeedWithMetaParserController<T> extends FeedParserController<T> {

    protected final IRecordWithMetadataParser<T> dataParser;

    public FeedWithMetaParserController(IRecordWithMetadataParser<T> dataParser) throws HyracksDataException {
        super(dataParser);
        this.dataParser = dataParser;
    }

    @Override
    public void addMetaPart(ArrayTupleBuilder tb, IRawRecord<? extends T> record) throws HyracksDataException {
        dataParser.parseMeta(tb.getDataOutput());
        tb.addFieldEndOffset();
    }
}
