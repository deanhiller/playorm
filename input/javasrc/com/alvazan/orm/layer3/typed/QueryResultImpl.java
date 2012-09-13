package com.alvazan.orm.layer3.typed;

import java.util.List;

import com.alvazan.orm.api.z3api.QueryResult;
import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class QueryResultImpl implements QueryResult {

	private Cursor<IndexColumnInfo> cursor;
	private List<ViewInfo> aliases;

	public QueryResultImpl(Cursor<IndexColumnInfo> cursor,
			List<ViewInfo> aliases) {
		this.cursor = cursor;
		this.aliases = aliases;
	}

	public Cursor<IndexColumnInfo> getCursor() {
		return cursor;
	}

	public List<ViewInfo> getViews() {
		return aliases;
	}
	
}
