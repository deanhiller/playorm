package com.alvazan.orm.layer5.query;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.base.Cursor;
import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.util.AbstractCursor;

public class CursorForOr extends AbstractCursor<IndexColumnInfo> {

	private Cursor<IndexColumnInfo> leftResults;
	private Cursor<IndexColumnInfo> rightResults;
	private Map<ByteArray, IndexColumnInfo> pksToAlreadyFound = new HashMap<ByteArray, IndexColumnInfo>();
	
	public CursorForOr(Cursor<IndexColumnInfo> leftResults2,
			Cursor<IndexColumnInfo> rightResults2) {
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
	protected com.alvazan.orm.util.AbstractCursor.Holder<IndexColumnInfo> nextImpl() {
		while(true) {
			Holder<IndexColumnInfo> nextFromCursor = nextFromCursor(leftResults);
			if(nextFromCursor == null)
				break;
			IndexColumnInfo result = nextFromCursor.getValue();
			pksToAlreadyFound.put(result.getPrimaryKey(), result);
			return new Holder<IndexColumnInfo>(result);
		}
		
		//NOW, as we go through the results on the right side, make sure we filter out 
		//duplicate primary keys by checking ones that we already returned.
		while(true) {
			Holder<IndexColumnInfo> nextFromCursor = nextFromCursor(rightResults);
			if(nextFromCursor == null)
				break;
			IndexColumnInfo lastCachedResult = nextFromCursor.getValue();
			IndexColumnInfo found = pksToAlreadyFound.get(lastCachedResult.getPrimaryKey());
			if(found != null) {
				found.setNextOrColumn(lastCachedResult);
			} else {
				return new Holder<IndexColumnInfo>(lastCachedResult);
			}
		}
		
		return null;
	}

}
