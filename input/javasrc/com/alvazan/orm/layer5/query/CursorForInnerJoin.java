package com.alvazan.orm.layer5.query;

import java.util.ArrayList;
import java.util.List;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;

public class CursorForInnerJoin extends AbstractCursor<IndexColumnInfo> {

	private AbstractCursor<IndexColumnInfo> rightResults;
	private ScanInfo scanInfo;
	private NoSqlSession session;
	private Integer batchSize;
	private AbstractCursor<IndexColumn> cachedResults;
	
	public CursorForInnerJoin(AbstractCursor<IndexColumnInfo> rightResults,
			ScanInfo scanInfo, NoSqlSession session, Integer batchSize) {
		this.rightResults = rightResults;
		this.scanInfo = scanInfo;
		this.session = session;
		this.batchSize = batchSize;
	}

	@Override
	public void beforeFirst() {
		this.rightResults.beforeFirst();
		cachedResults = null;
	}

	@Override
	public Holder<IndexColumnInfo> nextImpl() {
		if(cachedResults != null) {
			Holder<IndexColumn> next = cachedResults.nextImpl();
			if(next != null) {
				IndexColumnInfo info = new IndexColumnInfo();
				info.setPrimary(next.getValue());
				return new Holder<IndexColumnInfo>(info);
			}
			//we need to fetch more...
		}
		
		//HERE we want to batch for performance
		List<byte[]> values = new ArrayList<byte[]>();

		//optimize by fetching LOTS of keys at one time up to batchSize
		int counter = 0;
		while(rightResults.next()) {
			if(batchSize != null && batchSize.intValue() < counter)
				break;
			
			IndexColumnInfo current = rightResults.getCurrent();
			values.add(current.getPrimary().getPrimaryKey());
			counter++;
		}

		cachedResults = session.scanIndex(scanInfo, values);
		
		com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<IndexColumn> next = cachedResults.nextImpl();
		if(next == null)
			return null;

		IndexColumnInfo info = new IndexColumnInfo();
		info.setPrimary(next.getValue());
		return new Holder<IndexColumnInfo>(info);
	}

}
