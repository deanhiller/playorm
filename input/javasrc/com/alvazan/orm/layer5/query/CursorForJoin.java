package com.alvazan.orm.layer5.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;
import com.alvazan.orm.parser.antlr.JoinType;

public class CursorForJoin extends AbstractCursor<IndexColumnInfo> {

	private DirectCursor<IndexColumnInfo> rightResults;
	private ScanInfo scanInfo;
	private NoSqlSession session;
	private Integer batchSize;
	private DirectCursor<IndexColumn> cachedFromNewView;
	private ViewInfo newView;
	private ViewInfo rightView;
	private Iterator<IndexColumnInfo> cachedFromRightResults;
	private IndexColumnInfo lastCachedRightSide;
	private DboColumnMeta colMeta;

	public CursorForJoin(ViewInfo view, ViewInfo rightView, DirectCursor<IndexColumnInfo> rightResults,
			JoinType joinType) {
		Precondition.check(view, "view");
		Precondition.check(rightView, "rightView");
		Precondition.check(rightResults, "rightResults");
		Precondition.check(joinType, "joinType");
		this.newView = view;
		this.rightView = rightView;
		this.rightResults = rightResults;
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
		
		return fetchAnother();
	}

	private Holder<IndexColumnInfo> fetchAnother() {
		//HERE we want to batch for performance
		List<byte[]> values = new ArrayList<byte[]>();
		List<IndexColumnInfo> rightListResults = new ArrayList<IndexColumnInfo>();
				
		//optimize by fetching LOTS of keys at one time up to batchSize
		buildValuesToFind(values, rightListResults);
		
		cachedFromRightResults = rightListResults.iterator();
		cachedFromNewView = session.scanIndex(scanInfo, values);
		
		Holder<IndexColumn> next = cachedFromNewView.nextImpl();
		if(next == null)
			return null;
		else if(lastCachedRightSide == null)
			lastCachedRightSide = cachedFromRightResults.next();
		
		return createResult(next);
	}

	private void buildValuesToFind(List<byte[]> values,
			List<IndexColumnInfo> rightListResults) {
		//boolean rightResultsExhausted = false;
		int counter = 0;
		while(true) {
			Holder<IndexColumnInfo> rightHolder = rightResults.nextImpl();
			if(rightHolder == null) {
			//	rightResultsExhausted = true;
				break;
			} else if(batchSize != null && batchSize.intValue() < counter)
				break;
			
			IndexColumnInfo rightResult = rightHolder.getValue();
			ByteArray pk = rightResult.getPrimaryKey(rightView);
			values.add(pk.getKey());
			rightListResults.add(rightResult);
			counter++;
		}
		
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
		
		info.putIndexNode(newView, indCol, colMeta);
		return new Holder<IndexColumnInfo>(info);
	}

	public void setColMeta(DboColumnMeta col) {
		this.colMeta = col;
	}

}
