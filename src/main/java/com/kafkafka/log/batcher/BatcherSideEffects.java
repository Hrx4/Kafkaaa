package com.kafkafka.log.batcher;

import java.util.List;

public interface BatcherSideEffects {
    /**
     * Called when a batch has been flushed to disk.
     * @param offsets list of [logicalOffset, fileWriteOffset] pairs
     */
    void onBatchDone(List<int[]> offsets);
}
