package com.alvazan.orm.layer9z.spi.db.inmemory;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;

public class ScanInMemoryDbCollectin extends AbstractCursor<IndexColumn> {
	private Integer batchSize;
	private BatchListener batchListener;
	private ListIterator<IndexColumn> cachedRows;
	private Table indTable;
	private boolean needToGetBatch;

	public ScanInMemoryDbCollectin(Integer batchSize, BatchListener list,
			Table indTable) {
		this.batchSize = batchSize;
		this.batchListener = list;
		this.indTable = indTable;
		this.needToGetBatch = true;
		this.cachedRows = null;
		beforeFirst();
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "CursorForNonIndexColumn[" + tabs + "]";
		StringLocal.set(tabs.length());
		return retVal;
	}

	@Override
	public void beforeFirst() {
		cachedRows = null;
		needToGetBatch = true;
	}

	@Override
	public void afterLast() {
		cachedRows = null;
		needToGetBatch = true;
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<IndexColumn> nextImpl() {
		loadCache(false);
		if (cachedRows == null || !cachedRows.hasNext())
			return null;

		return new Holder<IndexColumn>(cachedRows.next());
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<IndexColumn> previousImpl() {
		loadCache(true);
		if (cachedRows == null || !cachedRows.hasPrevious())
			return null;
		return new Holder<IndexColumn>(cachedRows.previous());
	}

	private void loadCache(boolean reverse) {
		if (cachedRows != null && cachedRows.hasNext())
			return; // There are more rows so return and the code will return
					// the next result from cache
		if (needToGetBatch) {
			if (batchListener != null)
				batchListener.beforeFetchingNextBatch();
			Set<ByteArray> rows = indTable.findAllKeys();
			if (batchListener != null)
				batchListener.afterFetchingNextBatch(rows.size());
			List<IndexColumn> finalRes = new ArrayList<IndexColumn>();
			fillinCache(finalRes, rows);
			needToGetBatch = false;
			if (reverse) {
				while (cachedRows.hasNext())
					cachedRows.next();
			}
		}
	}

	private void fillinCache(List<IndexColumn> finalRes, Set<ByteArray> cursor) {
		if (cursor.size() == 0) {
			cachedRows = new ArrayList<IndexColumn>().listIterator();
		} else {
			for (ByteArray row : cursor) {
				IndexColumn c = convertToIndexColFromInId(row);
				finalRes.add(c);
			}
			cachedRows = finalRes.listIterator();
		}
	}

	private IndexColumn convertToIndexColFromInId(ByteArray rowKey) {
		IndexColumn c = new IndexColumn();
		c.setPrimaryKey(rowKey.getKey());
		c.setIndexedValue(rowKey.getKey());
		return c;
	}

}
