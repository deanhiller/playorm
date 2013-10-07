package com.alvazan.orm.layer9z.spi.db.cassandracql3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.datastax.driver.core.ResultSet;

public class CursorOfFutures extends AbstractCursor<IndexColumn> {

	private StartQueryListener queryListener;
	private BatchListener batchListener;
	private Iterator<Future<ResultSet>> theOneBatch;
	private boolean needToGetBatch;
	private String indTable;
	private ListIterator<IndexColumn> cachedLastCols;
	
	public CursorOfFutures(StartQueryListener l, BatchListener listener, ScanInfo scanInfo) {
		Precondition.check(l,"l");
		this.queryListener = l;
		this.batchListener = listener;
		this.indTable = scanInfo.getIndexColFamily();
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
	public void afterLast() {
		needToGetBatch = true;
	}

	@Override
	public Holder<IndexColumn> nextImpl() {
		if(batchListener != null)
			batchListener.beforeFetchingNextBatch();
		loadBatchIfNeeded();
		if(cachedLastCols != null && cachedLastCols.hasNext()) {
		    IndexColumn indexedCol = cachedLastCols.next();			 
			return new Holder<IndexColumn>(indexedCol);
		}
		
		while(true) {
			if(!theOneBatch.hasNext())
				return null;			

			Future<ResultSet> future = theOneBatch.next();
			ResultSet results = get(future);	
			cachedLastCols = new ArrayList<IndexColumn>().listIterator();

			if(!results.isExhausted()) {
			    com.datastax.driver.core.Row row = results.one();
			    IndexColumn indexCol = Cql3Util.convertToIndexCol(row, indTable);
			    cachedLastCols.add(indexCol);
				if(batchListener != null)
					batchListener.afterFetchingNextBatch(10);
				return new Holder<IndexColumn>(indexCol);
			}
		}
	}
	
	@Override
	public Holder<IndexColumn> previousImpl() {
		if(batchListener != null)
			batchListener.beforeFetchingNextBatch();
		loadBatchIfNeeded();
		if(cachedLastCols != null && cachedLastCols.hasPrevious()) {
		    IndexColumn indexedCol = cachedLastCols.previous();
			return new Holder<IndexColumn>(indexedCol);
		}
		
		while(true) {
			if(!theOneBatch.hasNext())
				return null;			
            Future<ResultSet> future = theOneBatch.next();
            ResultSet results = get(future);    
            cachedLastCols = new ArrayList<IndexColumn>().listIterator();

            while(cachedLastCols.hasNext())cachedLastCols.next();


			if(cachedLastCols.hasPrevious()) {
			    IndexColumn indexCol = cachedLastCols.previous();
					if(batchListener != null)
					batchListener.afterFetchingNextBatch(10);
				return new Holder<IndexColumn>(indexCol);
			}
		}
	}

	private void loadBatchIfNeeded() {
		if(needToGetBatch) {
			theOneBatch = queryListener.start().iterator();
			needToGetBatch = false;
		}
	}
	
	private ResultSet get(Future<ResultSet> f) {
		try {
			return (ResultSet) f.get();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}
}
