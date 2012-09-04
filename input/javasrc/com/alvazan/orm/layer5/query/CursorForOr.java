package com.alvazan.orm.layer5.query;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.AbstractCursor;
import com.alvazan.orm.api.z8spi.conv.ByteArray;

public class CursorForOr extends AbstractCursor<IndexColumnInfo> {

	private AbstractCursor<IndexColumnInfo> leftResults;
	private AbstractCursor<IndexColumnInfo> rightResults;
	private Map<ByteArray, IndexColumnInfo> pksToAlreadyFound = new HashMap<ByteArray, IndexColumnInfo>();
	
	public CursorForOr(AbstractCursor<IndexColumnInfo> leftResults2,
			AbstractCursor<IndexColumnInfo> rightResults2) {
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
	public com.alvazan.orm.api.z8spi.AbstractCursor.Holder<IndexColumnInfo> nextImpl() {
		while(true) {
			Holder<IndexColumnInfo> nextFromCursor = leftResults.nextImpl();
			if(nextFromCursor == null)
				break;
			IndexColumnInfo result = nextFromCursor.getValue();
			pksToAlreadyFound.put(result.getPrimaryKey(), result);
			return new Holder<IndexColumnInfo>(result);
		}
		
		//NOW, as we go through the results on the right side, make sure we filter out 
		//duplicate primary keys by checking ones that we already returned.
		while(true) {
			Holder<IndexColumnInfo> nextFromCursor = rightResults.nextImpl();
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
