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
import com.alvazan.orm.api.z8spi.meta.ViewInfo;
import com.alvazan.orm.parser.antlr.JoinType;

public class CursorForJoin extends AbstractCursor<IndexColumnInfo> {

	private AbstractCursor<IndexColumnInfo> rightResults;
	private ScanInfo scanInfo;
	private NoSqlSession session;
	private Integer batchSize;
	private AbstractCursor<IndexColumn> cachedFromNewView;
	private ViewInfo newView;
	private ViewInfo rightView;
	private Iterator<IndexColumnInfo> cachedFromRightResults;
	private IndexColumnInfo lastCachedRightSide;
	private JoinType joinType;
	private boolean alreadyRan;

	public CursorForJoin(ViewInfo view, ViewInfo rightView, AbstractCursor<IndexColumnInfo> rightResults,
			JoinType joinType) {
		this.newView = view;
		this.rightView = rightView;
		this.rightResults = rightResults;
		this.joinType = joinType;
	}
	
	public void setScanInfo(ScanInfo scanInfo) {
		this.scanInfo = scanInfo;
	}

	public void setSession(NoSqlSession session) {
		this.session = session;
	}

	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
	}

	@Override
	public void beforeFirst() {
		this.rightResults.beforeFirst();
		cachedFromNewView = null;
		alreadyRan = false;
	}

	@Override
	public Holder<IndexColumnInfo> nextImpl() {
		if(cachedFromNewView != null) {
			Holder<IndexColumn> next = cachedFromNewView.nextImpl();
			if(next != null) {
				return createResult(next);
			}
			//we need to fetch more...
		}
		
		//HERE we want to batch for performance
		List<byte[]> values = new ArrayList<byte[]>();
		List<IndexColumnInfo> rightListResults = new ArrayList<IndexColumnInfo>();
				
		//optimize by fetching LOTS of keys at one time up to batchSize
		boolean rightResultsExhausted = false;
		int counter = 0;
		while(true) {
			Holder<IndexColumnInfo> rightHolder = rightResults.nextImpl();
			if(rightHolder == null) {
				rightResultsExhausted = true;
				break;
			} else if(batchSize != null && batchSize.intValue() < counter)
				break;
			
			IndexColumnInfo rightResult = rightHolder.getValue();
			ByteArray pk = rightResult.getPrimaryKey(rightView);
			values.add(pk.getKey());
			rightListResults.add(rightResult);
			counter++;
		}
		
		//When we do the VERY last batch, tack on the null for outer joins as well to find pks that had null right sides
		if(rightResultsExhausted && joinType == JoinType.LEFT_OUTER && !alreadyRan) {
			//we need to add null to the query type to find in the index the values where it has no right side
			values.add(null);
			alreadyRan = true;
		}
		
		cachedFromRightResults = rightListResults.iterator();
		cachedFromNewView = session.scanIndex(scanInfo, values);
		
		com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<IndexColumn> next = cachedFromNewView.nextImpl();
		if(next == null)
			return null;
		else if(lastCachedRightSide == null)
			lastCachedRightSide = cachedFromRightResults.next();
		
		return createResult(next);
	}

	private Holder<IndexColumnInfo> createResult(
			com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<IndexColumn> next) {
		IndexColumn indCol = next.getValue();
		IndexColumnInfo info = new IndexColumnInfo();		
		
		if(cachedFromRightResults.hasNext()) {
			//We need to compare lastCachedRightSide with our incoming to make see if they pair up
			//or else move to the next right side answer.
			ByteArray pkOfRightView = lastCachedRightSide.getPrimaryKey(rightView);
			ByteArray valueOfLeft = new ByteArray(indCol.getIndexedValue());
			if(!valueOfLeft.equals(pkOfRightView)) {
				lastCachedRightSide = cachedFromRightResults.next();
			}		
	
			info.mergeResults(lastCachedRightSide);
		}
		
		info.putIndexNode(newView, indCol);
		return new Holder<IndexColumnInfo>(info);
	}

}
