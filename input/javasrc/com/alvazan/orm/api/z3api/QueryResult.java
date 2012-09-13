package com.alvazan.orm.api.z3api;

import java.util.List;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public interface QueryResult {

	Cursor<IndexColumnInfo> getCursor();

	List<ViewInfo> getAliases();

}
