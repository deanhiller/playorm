package com.alvazan.orm.layer9z.spi.db.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.action.IndexColumn;

import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;

import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;

public class CursorOfHbaseIndexes extends AbstractCursor<IndexColumn> {

	private HTableInterface hTable;
	private byte[] rowKey;
	private Integer batchSize;
	private BatchListener batchListener;
	private ListIterator<KeyValue> cachedRows;
	private String indexTableName;
	private boolean needToGetBatch;
	private Key from;
	private Key to;

	public CursorOfHbaseIndexes(byte[] rowKeys, Integer batchSize,
			BatchListener list, String indexTableName, Key from, Key to) {
		this.rowKey = rowKeys;
		this.batchSize = batchSize;
		this.batchListener = list;
		this.indexTableName = indexTableName;
		this.from = from;
		this.to = to;
		this.needToGetBatch = true;
		this.cachedRows = null;
		beforeFirst();
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String keys = "" + rowKey;
		String retVal = "CursorOfIndexes[" + tabs + keys + tabs + "]";
		StringLocal.set(tabs.length());
		return retVal;
	}

	public void setupMore(HTableInterface keyspace, DboColumnMeta colMeta) {
		if (keyspace == null)
			throw new IllegalArgumentException("DB was null");
		this.hTable = keyspace;
		beforeFirst();
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
		IndexColumn indexCol = convertToIndexCol(cachedRows.next());
		return new Holder<IndexColumn>(indexCol);
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<IndexColumn> previousImpl() {
		loadCache(true);
		if (cachedRows == null || !cachedRows.hasPrevious())
			return null;
		IndexColumn indexCol = convertToIndexCol(cachedRows.previous());
		return new Holder<IndexColumn>(indexCol);
	}

	private void loadCache(boolean reverse) {
		if (cachedRows != null && cachedRows.hasNext())
			return; // There are more rows so return and the code will return
					// the next result from cache

		if (needToGetBatch) {
			if (batchListener != null)
				batchListener.beforeFetchingNextBatch();

			byte[] endColumn = null;
			Filter f = null;
			byte[] startColumn = null;
			byte[] family = Bytes.toBytes(indexTableName);
			Scan scan = new Scan(rowKey, rowKey);
			scan.addFamily(family);
			boolean fromInclusive = false, toInclusive = false;

			if (from != null) {
				startColumn = from.getKey();
			}
			if (from.isInclusive()&& from != null)
				fromInclusive = true;
			if (to != null) {
				endColumn = to.getKey();
			}
			if (to.isInclusive()&& to != null)
				toInclusive = true;
			f = new ColumnRangeFilter(startColumn, fromInclusive, endColumn, toInclusive);
			scan.setFilter(f);
			if (batchSize != null)
				scan.setBatch(batchSize); // set this if there could be many columns returned
			ResultScanner rs;
			List<KeyValue> finalRes = new ArrayList<KeyValue>();
			try {
				rs = hTable.getScanner(scan);

				for (Result r = rs.next(); r != null; r = rs.next()) {
					for (KeyValue kv : r.raw()) {
						finalRes.add(kv);
					}
				}
				rs.close();
				if (batchListener != null)
					batchListener.afterFetchingNextBatch(2);

			} catch (IOException e) {
				// TODO Auto-generated catch block
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

	private void fillinCache(List<KeyValue> finalRes) {
		if (finalRes == null) {
			cachedRows = new ArrayList<KeyValue>().listIterator();
		} else {
			cachedRows = finalRes.listIterator();
		}
	}

	public static IndexColumn convertToIndexCol(KeyValue col) {
		byte[] indValue = col.getQualifier();
		byte[] pk = col.getValue();
		IndexColumn c = new IndexColumn();
		// c.setColumnName(columnName); Will we ever need this now?
		if (pk != null) {
			c.setPrimaryKey(pk);
		}
		if (indValue != null) {
			c.setIndexedValue(indValue);
		}
		c.setValue(null);
		return c;
	}
}
