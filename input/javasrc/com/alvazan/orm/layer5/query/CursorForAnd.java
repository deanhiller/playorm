package com.alvazan.orm.layer5.query;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.conv.Precondition;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class CursorForAnd implements DirectCursor<IndexColumnInfo> {

	private DirectCursor<IndexColumnInfo> leftResults;
	private DirectCursor<IndexColumnInfo> rightResults;
	private ViewInfo leftView;
	private ViewInfo rightView;
	private Map<ByteArray, IndexColumnInfo> cachedMap;
	private boolean ranFullInnerLoopTo500Once = false;
	
	public CursorForAnd(ViewInfo leftView2, DirectCursor<IndexColumnInfo> leftResults,
			ViewInfo rightView2, DirectCursor<IndexColumnInfo> rightResults) {
		Precondition.check(leftView2, "leftView2");
		Precondition.check(leftResults, "leftResults");
		Precondition.check(rightView2, "rightView2");
		Precondition.check(rightResults, "rightResults");
		this.leftView = leftView2;
		this.rightView = rightView2;
		this.leftResults = leftResults;
		this.rightResults = rightResults;
	}

	@Override
	public Holder<IndexColumnInfo> nextImpl() {
		while(true) {
			Holder<IndexColumnInfo> nextFromCursor = leftResults.nextImpl();
			if(nextFromCursor == null)
				break;
			IndexColumnInfo next = nextFromCursor.getValue();

			if(cachedMap != null) {
				Holder<IndexColumnInfo> result = quickHashLookup(next);
				if(result != null)
					return result;
			} else if(!ranFullInnerLoopTo500Once) {
				rightResults.beforeFirst();
				Holder<IndexColumnInfo> result = runFullInnerLoopOnce(next);
				if(result != null)
					return result;				
			} else {
				//Need to change to lookahead joining here as well as switch join types on the fly
				//This stinks as we have to re-read from the database every 1000 rows!!!! We should find out if
				//we can do any kind of counting or something so we don't have to go back to database every time
				rightResults.beforeFirst();
				Holder<IndexColumnInfo> result = runInnerLoop(next);
				if(result != null)
					return result;
			}
		}
		return null;
	}

	private Holder<IndexColumnInfo> runFullInnerLoopOnce(IndexColumnInfo next) {
		Map<ByteArray, IndexColumnInfo> pkToRightSide = new HashMap<ByteArray, IndexColumnInfo>();
		Holder<IndexColumnInfo> matchedResult = null;
		while(true) {
			Holder<IndexColumnInfo> nextFromCursor = rightResults.nextImpl();
			if(nextFromCursor == null)
				break;
			IndexColumnInfo andedInfo = nextFromCursor.getValue();
			ByteArray key1 = next.getPrimaryKey(leftView);
			ByteArray key2 = andedInfo.getPrimaryKey(rightView);
			if(pkToRightSide.size() < 500)
				pkToRightSide.put(key2, andedInfo);
			if(matchedResult == null && key1.equals(key2)) {
				next.mergeResults(andedInfo);
				matchedResult = new Holder<IndexColumnInfo>(next);
			}
			
			if(matchedResult != null && pkToRightSide.size()>=500) {
				//we have exceeded cache size so we can't switch to a Hash join :(
				return matchedResult;
			}
		}
		
		if(pkToRightSide.size() < 500)
			cachedMap = pkToRightSide;
		return matchedResult;
	}

	private Holder<IndexColumnInfo> quickHashLookup(IndexColumnInfo next) {
		ByteArray key1 = next.getPrimaryKey(leftView);
		IndexColumnInfo andedInfo = cachedMap.get(key1);
		if(andedInfo == null)
			return null;
		next.mergeResults(andedInfo);
		return new Holder<IndexColumnInfo>(next);
	}

	private Holder<IndexColumnInfo> runInnerLoop(IndexColumnInfo next) {
		while(true) {
			Holder<IndexColumnInfo> nextFromCursor = rightResults.nextImpl();
			if(nextFromCursor == null)
				break;
			IndexColumnInfo andedInfo = nextFromCursor.getValue();
			ByteArray key1 = next.getPrimaryKey(leftView);
			ByteArray key2 = andedInfo.getPrimaryKey(rightView);
			if(key1.equals(key2)) {
				next.mergeResults(andedInfo);
				return new Holder<IndexColumnInfo>(next);
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
