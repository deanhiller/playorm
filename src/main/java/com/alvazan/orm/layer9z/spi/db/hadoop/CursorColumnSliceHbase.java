package com.alvazan.orm.layer9z.spi.db.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;

import com.alvazan.orm.api.z8spi.action.Column;

class CursorColumnSliceHbase<T> extends AbstractCursor<T> {

	private BatchListener batchListener;
	private Integer batchSize;
	private HTableInterface hTable;
	private int pointer = -1;
	private DboTableMeta colFamily;
	private List<KeyValue> subList;
	private byte[] from;
	private byte[] to;
	private byte[] rowKey;
	private Boolean forward = null;

	public CursorColumnSliceHbase(DboTableMeta colFamily,
			BatchListener bListener, Integer batchSize, HTableInterface hTable,
			byte[] rowKey, byte[] from, byte[] to) {
		this.batchListener = bListener;
		this.batchSize = batchSize;
		this.hTable = hTable;
		this.from = from;
		this.to = to;
		this.rowKey = rowKey;
		this.colFamily = colFamily;
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
		fetchMoreResultsImpl();
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
			throw new IllegalStateException(
					"You must call beforeFirst to traverse the cursor forward, you cannot call next after calling previous due to limitations of talking to noSql apis");
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
		if(subList != null){
				if(pointer < subList.size()-1)
				return; 
			else if(batchSize == null) 
				return;
			else if(subList.size() < batchSize) {
				return;
			}
		}
		
       pointer = -1;
		
		if(batchListener != null)
			batchListener.beforeFetchingNextBatch();
		if (subList != null)
			return;

			if (batchListener != null)
				batchListener.beforeFetchingNextBatch();
			byte[] family = Bytes.toBytes(colFamily.getColumnFamily());
			Scan scan = new Scan(rowKey, rowKey);
			scan.addFamily(family);
			Filter f = new ColumnRangeFilter(from, true, to, true);
			scan.setFilter(f);
			if (batchSize != null)
				scan.setBatch(batchSize); // set this if there could be many columns returned
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
	/*private ValueFilter createFilterFrom() {
		CompareFilter.CompareOp fromInclusive1 = CompareOp.GREATER;
		BinaryComparator startColumn = null;
		if (from != null) {
			startColumn = new BinaryComparator(from);
			//if (from.isInclusive())
				fromInclusive1 = CompareOp.GREATER_OR_EQUAL;
		}
		return new ValueFilter(fromInclusive1, startColumn);
	}

	private ValueFilter createFilterTo() {
		CompareFilter.CompareOp toInclusive1 = CompareOp.LESS;
		BinaryComparator endColumn = null;
		if (to != null) {
			endColumn = new BinaryComparator(to);
			//if (to.isInclusive())
				toInclusive1 = CompareOp.LESS_OR_EQUAL;
		}
		return new ValueFilter(toInclusive1, endColumn);
	}
*/
	private com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<T> buildHolder(
			KeyValue column) {
		byte[] name = column.getQualifier();
		byte[] val = column.getValue();
		Column c = new Column();
		c.setName(name);
		if (val.length != 0)
			c.setValue(val);
		return new Holder<T>((T) c);

	}
}
