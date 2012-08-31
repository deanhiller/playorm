package com.alvazan.orm.layer5.query;

import com.alvazan.orm.api.base.Cursor;
import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.util.AbstractCursor;

public class CursorForAnd extends AbstractCursor<IndexColumnInfo> {

	private Cursor<IndexColumnInfo> leftResults;
	private Cursor<IndexColumnInfo> rightResults;
	
	public CursorForAnd(Cursor<IndexColumnInfo> leftResults,
			Cursor<IndexColumnInfo> rightResults) {
		this.leftResults = leftResults;
		this.rightResults = rightResults;
	}

	@Override
	public Holder<IndexColumnInfo> nextImpl() {
		while(leftResults.hasNext()) {
			IndexColumnInfo next = leftResults.next();
			//This stinks as we have to re-read from the database every 1000 rows!!!! We should find out if
			//we can do any kind of counting or something so we don't have to go back to database every time
			rightResults.beforeFirst();
			while(rightResults.hasNext()) {
				IndexColumnInfo andedInfo = rightResults.next();
				ByteArray key1 = next.getPrimaryKey();
				ByteArray key2 = andedInfo.getPrimaryKey();
				if(key1.equals(key2)) {
					next.setNextAndedColumn(andedInfo);
					return new Holder<IndexColumnInfo>(next);
				}
			}
		}
		return null;
	}


	@Override
	public void beforeFirst() {
		leftResults.beforeFirst();
		rightResults.beforeFirst();
	}

}
