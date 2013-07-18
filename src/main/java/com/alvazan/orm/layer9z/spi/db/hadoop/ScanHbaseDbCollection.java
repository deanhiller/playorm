package com.alvazan.orm.layer9z.spi.db.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;

public class ScanHbaseDbCollection extends AbstractCursor<IndexColumn> {
	private Integer batchSize;
	private BatchListener batchListener;
	private ListIterator<byte[]> cachedRows;
	private String indTable;
	private HTableInterface hTable;
	private boolean needToGetBatch;

	public ScanHbaseDbCollection(Integer batchSize, BatchListener list,
			String table, HTableInterface hTable) {
		this.batchSize = batchSize;
		this.batchListener = list;
		this.indTable = table;
		this.hTable = hTable;
		this.needToGetBatch = true;
		this.cachedRows = null;
		beforeFirst();
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "ScanHbaseDbCollection[" + tabs + "]";
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
		IndexColumn indexCol = convertToIndexColFromId(cachedRows.next());
		return new Holder<IndexColumn>(indexCol);
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<IndexColumn> previousImpl() {
		loadCache(true);
		if (cachedRows == null || !cachedRows.hasPrevious())
			return null;
		IndexColumn indexCol = convertToIndexColFromId(cachedRows.previous());
		return new Holder<IndexColumn>(indexCol);
	}

	private IndexColumn convertToIndexColFromId(byte[] col) {
		IndexColumn c = new IndexColumn();
		c.setPrimaryKey(col);
		c.setIndexedValue(col);
		return c;
	}

	private void loadCache(boolean reverse) {
		if (cachedRows != null && cachedRows.hasNext())
			return; // There are more rows so return and the code will return
					// the next result from cache
		byte[] family = Bytes.toBytes(indTable);
		Scan scan = new Scan();
		scan.addFamily(family);
		if (needToGetBatch) {
			if (batchListener != null)
				batchListener.beforeFetchingNextBatch();
			ResultScanner rs;
			List<byte[]> finalRes = new ArrayList<byte[]>();
			if (batchSize != null)
				scan.setBatch(batchSize);
			try {
				rs = hTable.getScanner(scan);
				for (Result r = rs.next(); r != null; r = rs.next()) {
					byte[] kv = r.getRow();
					finalRes.add(kv);

				}
				rs.close();
				if (batchListener != null)
					batchListener.afterFetchingNextBatch(2);

			} catch (IOException e) {
				e.printStackTrace();
			}

			fillinCache(finalRes);
			needToGetBatch = false;
			if (reverse) {
				while (cachedRows.hasNext())
					cachedRows.next();
			}
		}
	}

	private void fillinCache(List<byte[]> finalRes) {
		if (finalRes == null) {
			cachedRows = new ArrayList<byte[]>().listIterator();
		} else {
			cachedRows = finalRes.listIterator();
		}
	}

}
