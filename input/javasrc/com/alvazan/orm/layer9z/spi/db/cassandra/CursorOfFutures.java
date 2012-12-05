package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class CursorOfFutures extends AbstractCursor<IndexColumn> {

	private StartQueryListener columnSliceCb;
	private BatchListener batchListener;
	private Iterator<Future<OperationResult<ColumnList<byte[]>>>> theOneBatch;
	private boolean needToGetBatch;
	private Iterator<Column<byte[]>> cachedLastCols;
	
	public CursorOfFutures(StartQueryListener l, BatchListener listener) {
		Precondition.check(l,"l");
		this.columnSliceCb = l;
		this.batchListener = listener;
		beforeFirst();
	}
	
	@Override
	public String toString() {
		return "CursorOfFutures["+theOneBatch+"]";
	}

	@Override
	public void beforeFirst() {
		needToGetBatch = true;
	}

	@Override
	public Holder<IndexColumn> nextImpl() {
		if(batchListener != null)
			batchListener.beforeFetchingNextBatch();
		loadBatchIfNeeded();
		if(cachedLastCols != null && cachedLastCols.hasNext()) {
			Column<byte[]> col = cachedLastCols.next();
			IndexColumn indexedCol = CursorColumnSlice.convertToIndexCol(col);
			return new Holder<IndexColumn>(indexedCol);
		}
		
		while(true) {
			if(!theOneBatch.hasNext())
				return null;			
			Future<OperationResult<ColumnList<byte[]>>> future = theOneBatch.next();

			OperationResult<ColumnList<byte[]>> results = get(future);
			ColumnList<byte[]> columnList = results.getResult();
			cachedLastCols = columnList.iterator();

			if(cachedLastCols.hasNext()) {
				Column<byte[]> col = cachedLastCols.next();
				IndexColumn indexCol = CursorColumnSlice.convertToIndexCol(col);
	
				if(batchListener != null)
					batchListener.afterFetchingNextBatch(columnList.size());
				return new Holder<IndexColumn>(indexCol);
			}
		}
	}

	private void loadBatchIfNeeded() {
		if(needToGetBatch) {
			theOneBatch = columnSliceCb.start().iterator();
			needToGetBatch = false;
		}
	}
	
	private OperationResult<ColumnList<byte[]>> get(Future<OperationResult<ColumnList<byte[]>>> f) {
		try {
			return (OperationResult<ColumnList<byte[]>>) f.get();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}
}
