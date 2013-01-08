package com.alvazan.orm.layer9z.spi.db.cassandra;

import input.javasrc.com.alvazan.orm.api.z8spi.action.Column;
import java.util.ListIterator;


public class OurColumnListIterator implements ListIterator<Column<byte[]>>{

	private ColumnList<byte[]> columns = null;
	private int currIndex=0;  //zero means *before* the zeroith element
	private boolean reversed = false;
	
	public OurColumnListIterator(ColumnList<byte[]> cols) {
		columns = cols;
	}
	
	public OurColumnListIterator(ColumnList<byte[]> cols, boolean reversed) {
		columns = cols;
		this.reversed = reversed;
	}

	@Override
	public boolean hasNext() {
		if (reversed)
			return hasPreviousImpl();
		return hasNextImpl();
	}

	@Override
	public boolean hasPrevious() {
		if (reversed)
			return hasNextImpl();
		return hasPreviousImpl();
	}

	@Override
	public Column<byte[]> next() {
		if (reversed)
			return previousImpl();
		return nextImpl();
	}

	@Override
	public int nextIndex() {
		if (reversed)
			return previousIndexImpl();
		return nextIndexImpl();
	}

	@Override
	public Column<byte[]> previous() {
		if (reversed)
			return nextImpl();
		return previousImpl();
	}

	@Override
	public int previousIndex() {
		if(reversed)
			return nextIndexImpl();
		return previousIndexImpl();
	}
	
	
	public boolean hasNextImpl() {
		return currIndex!=columns.size();
	}

	
	public boolean hasPreviousImpl() {
		return currIndex!=0;
	}

	
	public Column<byte[]> nextImpl() {
		if(currIndex==columns.size())
			return null;
		return columns.getColumnByIndex(currIndex++);
	}

	
	public int nextIndexImpl() {
		return currIndex;
	}

	
	public Column<byte[]> previousImpl() {
		if(currIndex<=0)
			return null;
		return columns.getColumnByIndex(--currIndex);
	}

	
	public int previousIndexImpl() {
		return currIndex-1;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("We don't support this");
		
	}

	@Override
	public void set(Column<byte[]> arg0) {
		throw new UnsupportedOperationException("We don't support this");
		
	}
	
	@Override
	public void add(Column<byte[]> arg0) {
		throw new UnsupportedOperationException("We don't support this");
		
	}
	
}

