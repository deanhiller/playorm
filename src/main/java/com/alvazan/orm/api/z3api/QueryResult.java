package com.alvazan.orm.api.z3api;

import java.util.List;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public interface QueryResult {

	/**
	 * This cursor is a cursor into JUST the row keys of the entites that match the query.
	 * @return A <code>Cursor</code> of IndexColumnInfo
	 */
	Cursor<IndexColumnInfo> getCursor();

	List<ViewInfo> getViews();

	/**
	 * This cursor on every cursor.next/cursor.getCurrent returns a List of the joined rows so you 
	 * could display in a command line or GUI tool the results(and page the results).
	 * @return A <code>Cursor</code> having list of TypedRow objects
	 */
	Cursor<List<TypedRow>> getAllViewsCursor();

	/**
	 * This cursor ONLY returns the primary View's rows and NOT the rows that were joined with though the
	 * primary view usually contains the other row keys in a ManyToOne or OneToOne case anyways.
	 * @return A <code>Cursor</code> 
	 */
	Cursor<KeyValue<TypedRow>> getPrimaryViewCursor();
	
	Iterable<KeyValue<TypedRow>> getPrimaryViewIter();
	
	/**
	 * Sometimes an Iterable is way more convenient
	 * @return An iterator over the elements in the QueryResult
	 */
	Iterable<List<TypedRow>> getAllViewsIter();
	
}
