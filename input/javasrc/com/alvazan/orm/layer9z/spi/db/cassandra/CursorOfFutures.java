package com.alvazan.orm.layer9z.spi.db.cassandra;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;

public class CursorOfFutures extends AbstractCursor<IndexColumn> {

	private StartQueryListener columnSliceCb;
	private BatchListener batchListener;
	private Iterator<Future<OperationResult<ColumnList<byte[]>>>> theOneBatch;
	
	public CursorOfFutures(StartQueryListener l, BatchListener listener) {
		Precondition.check(l,"l");
		this.columnSliceCb = l;
		this.batchListener = listener;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		theOneBatch = columnSliceCb.start().iterator();
	}

	@Override
	public Holder<IndexColumn> nextImpl() {
		if(!theOneBatch.hasNext())
			return null;
		Future<OperationResult<ColumnList<byte[]>>> future = theOneBatch.next();
		
		if(batchListener != null)
			batchListener.beforeFetchingNextBatch();
		OperationResult<ColumnList<byte[]>> results = get(future);
		ColumnList<byte[]> columnList = results.getResult();
		if(batchListener != null)
			batchListener.afterFetchingNextBatch(columnList.size());
		
		com.netflix.astyanax.model.Column<byte[]> col = columnList.iterator().next();
		IndexColumn indexCol = CursorColumnSlice.convertToIndexCol(col);
		
		return new Holder<IndexColumn>(indexCol);
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
