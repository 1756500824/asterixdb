package org.apache.asterix.external.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public interface Forwarder {
    void addTuple(ArrayTupleBuilder tb) throws HyracksDataException;

    void flush() throws HyracksDataException;

    void complete() throws HyracksDataException;
}
