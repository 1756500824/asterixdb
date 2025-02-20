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
package org.apache.asterix.external.input.record;

import org.apache.asterix.external.api.IRawRecord;

public class GenericRecord<T> implements IRawRecord<T> {

    private T record;

    //    private Class<T> type;

    public GenericRecord() {
    }

    public GenericRecord(T record) {
        this.record = record;
        //        this.type = record.g;
    }

    @Override
    public byte[] getBytes() {
        return null;
    }

    @Override
    public T get() {
        return record;
    }

    @Override
    public int size() {
        return -1;
    }

    @Override
    public void set(T record) {
        this.record = record;
    }

    @Override
    public void reset() {
    }

    @Override
    public String toString() {
        return record == null ? null : record.toString();
    }
}
