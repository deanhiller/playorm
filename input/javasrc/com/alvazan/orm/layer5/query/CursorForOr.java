package com.alvazan.orm.layer5.query;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class CursorForOr implements DirectCursor<IndexColumnInfo> {

	private DirectCursor<IndexColumnInfo> leftResults;
	private DirectCursor<IndexColumnInfo> rightResults;
	private Map<ByteArray, IndexColumnInfo> pksToAlreadyFound = new HashMap<ByteArray, IndexColumnInfo>();
	private ViewInfo leftView;
	private ViewInfo rightView;
	
	public CursorForOr(ViewInfo leftView, DirectCursor<IndexColumnInfo> leftResults2,
			ViewInfo rightView, DirectCursor<IndexColumnInfo> rightResults2) {
		this.leftView = leftView;
		this.rightView = rightView;
		this.leftResults = leftResults2;
		this.rightResults = rightResults2;
	}

	@Override
	public void beforeFirst() {
		leftResults.beforeFirst();
		rightResults.beforeFirst();
		pksToAlreadyFound.clear();
	}
	
	@Override
	public Holder<IndexColumnInfo> nextImpl() {
		while(true) {
			Holder<IndexColumnInfo> nextFromLeftCursor = leftResults.nextImpl();
			if(nextFromLeftCursor == null)
				break;
			IndexColumnInfo leftResult = nextFromLeftCursor.getValue();
			ByteArray pk = leftResult.getPrimaryKey(leftView);
			pksToAlreadyFound.put(pk, leftResult);
			return new Holder<IndexColumnInfo>(leftResult);
		}
		
		//NOW, as we go through the results on the right side, make sure we filter out 
		//duplicate primary keys by checking ones that we already returned.
		while(true) {
			Holder<IndexColumnInfo> fromRightCursor = rightResults.nextImpl();
			if(fromRightCursor == null)
				break;
			IndexColumnInfo rightResult = fromRightCursor.getValue();
			IndexColumnInfo found = pksToAlreadyFound.get(rightResult.getPrimaryKey(rightView));
			if(found != null) {
				found.mergeResults(rightResult);
			} else {
				return new Holder<IndexColumnInfo>(rightResult);
			}
		}
		
		return null;
	}

}
