package com.alvazan.orm.layer5.query;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class CursorForOr implements DirectCursor<IndexColumnInfo> {

	private DirectCursor<IndexColumnInfo> leftResults;
	private DirectCursor<IndexColumnInfo> rightResults;
	private ViewInfo leftView;
	private ViewInfo rightView;
	private Holder<IndexColumnInfo> currLeftVal;
	private Holder<IndexColumnInfo> currRightVal;
	
	public CursorForOr(ViewInfo leftView, DirectCursor<IndexColumnInfo> leftResults2,
			ViewInfo rightView, DirectCursor<IndexColumnInfo> rightResults2) {
		Precondition.check(leftView, "leftView");
		Precondition.check(leftResults2, "leftResults2");
		Precondition.check(rightView, "rightView");
		Precondition.check(rightResults2, "rightResults2");
		this.leftView = leftView;
		this.rightView = rightView;
		this.leftResults = leftResults2;
		this.rightResults = rightResults2;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		leftResults.beforeFirst();
		rightResults.beforeFirst();
		currLeftVal = leftResults.nextImpl();
		currRightVal = rightResults.nextImpl();
	}
	
	@Override
	public void afterLast() {
		leftResults.afterLast();
		rightResults.afterLast();
		currLeftVal = leftResults.previousImpl();
		currRightVal = rightResults.previousImpl();
	}
	
	@Override
	public Holder<IndexColumnInfo> nextImpl() {
		return nextImpl(false);
	}
	
	@Override
	public Holder<IndexColumnInfo> previousImpl() {
		return nextImpl(true);
	}
	
	private Holder<IndexColumnInfo> nextImpl(boolean reverse) {
		if (currLeftVal != null && currRightVal != null) {
			int compareResult = currLeftVal.getValue().getPrimaryKey(leftView).compareTo(currRightVal.getValue().getPrimaryKey(rightView)); 
			if (reverse)
				compareResult = compareResult*-1;
			if (compareResult < 0) 
				return useLeft(reverse);
			else if (compareResult > 0)
				return useRight(reverse);
			else { // they are the same, use either, and skip the unused one since it's identical
				useRight(reverse);
				return useLeft(reverse);
			}
		}
		else {
			if (currLeftVal!=null) 
				return useLeft(reverse);
			else if (currRightVal!=null)
				return useRight(reverse);
			else return null;
		}
	}
	
	private Holder<IndexColumnInfo> useLeft(boolean reverse) {
		Holder<IndexColumnInfo> retVal = currLeftVal;
		if (reverse)
			currLeftVal = leftResults.previousImpl();
		else
			currLeftVal = leftResults.nextImpl();
		return retVal;
	}
	
	private Holder<IndexColumnInfo> useRight(boolean reverse) {
		Holder<IndexColumnInfo> retVal = currRightVal;
		if (reverse)
			currRightVal = rightResults.previousImpl();
		else
			currRightVal = rightResults.nextImpl();
		return retVal;
	}
}
