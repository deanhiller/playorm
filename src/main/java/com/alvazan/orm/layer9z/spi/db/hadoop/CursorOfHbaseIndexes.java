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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
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
			byte[] family = Bytes.toBytes(indexTableName);
			Scan scan = new Scan(rowKey, rowKey);
			scan.addFamily(family);
			FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			if (from != null) {
				ValueFilter fromfilter = createFilterFrom();
				list.addFilter(fromfilter);
			}
			if (to != null) {
				ValueFilter toFilter = createFilterTo();
				list.addFilter(toFilter);
			}
			if (!list.getFilters().isEmpty())
				scan.setFilter(list);
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

	private ValueFilter createFilterFrom() {
		CompareFilter.CompareOp fromInclusive1 = CompareOp.GREATER;
		BinaryComparator startColumn = null;
		if (from != null) {
			if ((indexTableName.equalsIgnoreCase("IntegerIndice")))
				startColumn = new BinaryComparator(conversionForHbase(from));
			else
				startColumn = new BinaryComparator(from.getKey());
			if (from.isInclusive())
				fromInclusive1 = CompareOp.GREATER_OR_EQUAL;
		}
		return new ValueFilter(fromInclusive1, startColumn);
	}

	private byte[] conversionForHbase(Key key) {
		byte[] byteArr = null;
		if (key != null && key.getKey() != null) {
			int tempInt = StandardConverters.convertFromBytes(Integer.class,key.getKey());
			tempInt ^= (1 << 31);
			byteArr = Bytes.toBytes(tempInt);
		}
		return byteArr;
	}

	private ValueFilter createFilterTo() {
		CompareFilter.CompareOp toInclusive1 = CompareOp.LESS;
		BinaryComparator endColumn = null;
		if (to != null) {
			if ((indexTableName.equalsIgnoreCase("IntegerIndice")))
				endColumn = new BinaryComparator(conversionForHbase(to));
			else
				endColumn = new BinaryComparator(to.getKey());

			if (to.isInclusive())
				toInclusive1 = CompareOp.LESS_OR_EQUAL;
		}
		return new ValueFilter(toInclusive1, endColumn);
	}

	private void fillinCache(List<KeyValue> finalRes) {
		if (finalRes == null) {
			cachedRows = new ArrayList<KeyValue>().listIterator();
		} else {
			cachedRows = finalRes.listIterator();
		}
	}

	public static IndexColumn convertToIndexCol(KeyValue col) {
		byte[] pk = col.getQualifier();
		byte[] indValue = col.getValue();
		IndexColumn c = new IndexColumn();
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
