package com.alvazan.orm.layer9z.spi.db.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;

public class CursorForHbaseValues extends AbstractCursor<IndexColumn> {

	private byte[] rowKey;
	private BatchListener batchListener;
	private ListIterator<IndexColumn> cachedRows;
	private String indexTableName;
	private boolean needToGetBatch;
	private List<byte[]> values;
	private HTableInterface htable;

	public CursorForHbaseValues(byte[] rowKeys, BatchListener list,
			String indexTableName, List<byte[]> keys, HTableInterface htable) {
		this.rowKey = rowKeys;
		this.batchListener = list;
		this.indexTableName = indexTableName;
		this.needToGetBatch = true;
		this.cachedRows = null;
		this.values = keys;
		this.htable = htable;
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String keys = "" + rowKey;
		String retVal = "CursorForValues(MongoDB)[" + tabs + keys + tabs + "]";
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
			Result[] result = null;
			byte[] family = Bytes.toBytes(indexTableName);
			List<Get> listGet = new ArrayList<Get>();
			for (byte[] key : values) {
				Get get = new Get(rowKey);
				CompareFilter.CompareOp equal = CompareOp.EQUAL;
				BinaryComparator endColumn1 = new BinaryComparator(key);
				ValueFilter filter = new ValueFilter(equal, endColumn1);
				get.setFilter(filter);
				get.addFamily(family);
				listGet.add(get);
			}
			try {
				result = htable.get(listGet);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			fillCache(result);
			needToGetBatch = false;
			if (reverse) {
				while (cachedRows.hasNext())
					cachedRows.next();
			}
		}
	}

	private void fillCache(Result[] results) {
		List<IndexColumn> finalRes = new ArrayList<IndexColumn>();
		if (results == null) {
			cachedRows = finalRes.listIterator();
		} else {
			for (Result result : results) {
				List<org.apache.hadoop.hbase.KeyValue> hKeyValue = result.list();
				if (hKeyValue != null && !hKeyValue.isEmpty()) {
					for (org.apache.hadoop.hbase.KeyValue keyValue : hKeyValue) {
						IndexColumn indCol = CursorOfHbaseIndexes
								.convertToIndexCol(keyValue);
						finalRes.add(indCol);
					}
				}
			}
			cachedRows = finalRes.listIterator();
		}
	}

}
