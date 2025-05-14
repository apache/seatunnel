package org.apache.seatunnel.connectors.seatunnel.deltalake.source.reader;

import io.delta.kernel.data.Row;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.utils.CloseableIterator;

import java.io.IOException;
import java.util.NoSuchElementException;

public class BatchToRowIterator implements CloseableIterator<Row> {
  private final CloseableIterator<FilteredColumnarBatch> batchIterator;
  private CloseableIterator<Row> currentRowIterator = null;

  public BatchToRowIterator(CloseableIterator<FilteredColumnarBatch> batchIterator) {
    this.batchIterator = batchIterator;
  }

  @Override
  public boolean hasNext() {
    while (currentRowIterator == null || !currentRowIterator.hasNext()) {
      if (!batchIterator.hasNext()) {
        return false;
      }
      FilteredColumnarBatch nextBatch = batchIterator.next();
      ColumnarBatch columnarBatch = nextBatch.getData();
      currentRowIterator = columnarBatch.getRows();
    }
    return true;
  }

  @Override
  public Row next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return currentRowIterator.next();
  }

  @Override
  public void close() throws IOException {
    batchIterator.close();
    currentRowIterator.close();
  }
}
