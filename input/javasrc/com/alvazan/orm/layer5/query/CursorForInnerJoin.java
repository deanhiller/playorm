package com.alvazan.orm.layer5.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.parser.antlr.ViewInfo;

public class CursorForInnerJoin extends AbstractCursor<IndexColumnInfo> {

	private AbstractCursor<IndexColumnInfo> rightResults;
	private ScanInfo scanInfo;
	private NoSqlSession session;
	private Integer batchSize;
	private AbstractCursor<IndexColumn> cachedFromNewView;
	private ViewInfo newView;
	private ViewInfo rightView;
	private Iterator<IndexColumnInfo> cachedFromRightResults;

	public CursorForInnerJoin(ViewInfo view, ViewInfo rightView, AbstractCursor<IndexColumnInfo> rightResults,
			ScanInfo scanInfo, NoSqlSession session, Integer batchSize) {
		this.newView = view;
		this.rightView = rightView;
		this.rightResults = rightResults;
		this.scanInfo = scanInfo;
		this.session = session;
		this.batchSize = batchSize;
	}

	@Override
	public void beforeFirst() {
		this.rightResults.beforeFirst();
		cachedFromNewView = null;
	}

	@Override
	public Holder<IndexColumnInfo> nextImpl() {
		if(cachedFromNewView != null) {
			Holder<IndexColumn> next = cachedFromNewView.nextImpl();
			if(next != null) {
				IndexColumnInfo rightSide = cachedFromRightResults.next();
				rightSide.putIndexNode(newView, next.getValue());
				return new Holder<IndexColumnInfo>(rightSide);
			}
			//we need to fetch more...
		}
		
		//HERE we want to batch for performance
		List<byte[]> values = new ArrayList<byte[]>();
		List<IndexColumnInfo> rightListResults = new ArrayList<IndexColumnInfo>();
		
		//optimize by fetching LOTS of keys at one time up to batchSize
		int counter = 0;
		while(rightResults.next()) {
			if(batchSize != null && batchSize.intValue() < counter)
				break;
			
			IndexColumnInfo rightResult = rightResults.getCurrent();
			ByteArray pk = rightResult.getPrimaryKey(rightView);
			values.add(pk.getKey());
			rightListResults.add(rightResult);
			counter++;
		}

		cachedFromRightResults = rightListResults.iterator();
		cachedFromNewView = session.scanIndex(scanInfo, values);
		
		com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<IndexColumn> next = cachedFromNewView.nextImpl();
		if(next == null)
			return null;

		IndexColumnInfo rightSide = cachedFromRightResults.next();
		rightSide.putIndexNode(newView, next.getValue());
		return new Holder<IndexColumnInfo>(rightSide);
	}

}
