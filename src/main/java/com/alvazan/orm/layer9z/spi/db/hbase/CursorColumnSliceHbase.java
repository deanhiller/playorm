package com.alvazan.orm.layer9z.spi.db.hbase;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.ColumnSliceInfo;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;

import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;

class CursorColumnSliceHbase<T> extends AbstractCursor<T> {

	private BatchListener batchListener;
	private Integer batchSize;
	private HTableInterface hTable;
	private int pointer = -1;
	private ColumnSliceInfo sliceInfo;
	private List<KeyValue> subList;
	private byte[] from;
	private byte[] to;
	private byte[] rowKey;
	private Boolean forward = null;
	private HColumnDescriptor colDescriptor;
	private Class columnNameType=null;

	public CursorColumnSliceHbase(ColumnSliceInfo sliceInfo2,
			BatchListener bListener, Integer batchSize, HTableInterface hTable, HColumnDescriptor colDescriptor2) {
		this.batchListener = bListener;
		this.batchSize = batchSize;
		this.hTable = hTable;
		this.from = sliceInfo2.getFrom();
		this.to = sliceInfo2.getTo();
		this.colDescriptor=colDescriptor2;
		this.rowKey = sliceInfo2.getRowKey();
		this.sliceInfo = sliceInfo2;
		beforeFirst();
		
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String retVal = "CursorColumnSlice[" + tabs + tabs + "]";
		StringLocal.set(tabs.length());
		return retVal;
	}

	@Override
	public void beforeFirst() {
		pointer = -1;
		subList = null;
		forward = true;

	}

	@Override
	public void afterLast() {
	    pointer = -1;
		subList = null;
		forward = false;
   }

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<T> nextImpl() {
	    if (!forward)
			throw new IllegalStateException("You must call beforeFirst to traverse the cursor forward, you cannot call next after calling previous due to limitations of talking to noSql apis");
		fetchMoreResultsImpl();
		pointer++;
		if (pointer >= subList.size())
			return null; // no more results
		KeyValue column = subList.get(pointer);
		return buildHolder(column);

	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<T> previousImpl() {
		if (forward)
			throw new IllegalStateException(
					"You must call afterLast to traverse reverse.  You cannot call previous after calling next due to limitations of calling into noSQL apis");
		fetchMoreResultsImpl();
		pointer++;
		if (pointer >= subList.size())
			return null; // no more results
		KeyValue column = subList.get(pointer);
		return buildHolder(column);
	}

	private void fetchMoreResultsImpl() {
		if (subList != null) {
			if (pointer < subList.size()) {
				return;
			} else if (batchSize == null) {
				return;
			} else if (subList.size() < batchSize) {
				return;
			}
		}

		pointer = -1;

		if (batchListener != null)
			batchListener.beforeFetchingNextBatch();
		if (subList != null)
			return;

		if (batchListener != null)
			batchListener.beforeFetchingNextBatch();
		columnNameType = sliceInfo.getColumnNameType();

		byte[] family = Bytes.toBytes(colDescriptor.getNameAsString());
		Scan scan = new Scan(rowKey, rowKey);
		scan.addFamily(family);
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		if (from != null) {
			QualifierFilter fromfilter = createFilterFrom();
			list.addFilter(fromfilter);
		}
		if (to != null) {
			QualifierFilter toFilter = createFilterTo();
			list.addFilter(toFilter);
		}
		scan.setFilter(list);
		if (batchSize != null)
			scan.setBatch(batchSize); // / set this if there could be many
										// columns returned
		ResultScanner rs;
		subList = new ArrayList<KeyValue>();
		try {
			rs = hTable.getScanner(scan);
			for (Result r = rs.next(); r != null; r = rs.next()) {
				for (KeyValue kv : r.raw()) {
					subList.add(kv);
				}
			}
			rs.close();
			if (batchListener != null)
				batchListener.afterFetchingNextBatch(2);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private QualifierFilter createFilterFrom() {
		if (BigInteger.class.equals(columnNameType)) {
			int flippedFrom = StandardConverters.convertFromBytes(Integer.class, from);
			flippedFrom ^= (1 << 31);
			from = Bytes.toBytes(flippedFrom);
		}
		BinaryComparator startColumn = new BinaryComparator(from);
		CompareFilter.CompareOp  fromInclusive = CompareOp.GREATER_OR_EQUAL;
		return new QualifierFilter(fromInclusive, startColumn);
	}

	private QualifierFilter createFilterTo() {
		if (BigInteger.class.equals(columnNameType)) {
			int flippedTo = StandardConverters.convertFromBytes(Integer.class, to);
			flippedTo ^= (1 << 31);
			to = Bytes.toBytes(flippedTo);
		}
		BinaryComparator endColumn = new BinaryComparator(to);
		CompareFilter.CompareOp toInclusive = CompareOp.LESS_OR_EQUAL;
		return new QualifierFilter(toInclusive, endColumn);
	}
	private com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<T> buildHolder(
			KeyValue column) {
		byte[] name = column.getQualifier();
		if (BigInteger.class.equals(columnNameType)) {
			name = column.getQualifier();
			int flippedNumber = StandardConverters.convertFromBytes(
					Integer.class, name);
			flippedNumber ^= (1 << 31);
			name = Bytes.toBytes(flippedNumber);
		}
		byte[] val = column.getValue();
		Column c = new Column();
		c.setName(name);
		if (val.length != 0)
			c.setValue(val);
		return new Holder<T>((T) c);
	}
}
