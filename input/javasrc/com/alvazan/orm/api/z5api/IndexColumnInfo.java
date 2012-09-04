package com.alvazan.orm.api.z5api;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.parser.antlr.ViewInfo;

public class IndexColumnInfo {

	private Map<ViewInfo, IndexColumn> colNameToValue = new HashMap<ViewInfo, IndexColumn>();
	
	public void putIndexNode(ViewInfo viewInfo, IndexColumn indCol) {
		this.colNameToValue.put(viewInfo, indCol);
	}

	public IndexColumn getIndexNode(ViewInfo view) {
		return colNameToValue.get(view);
	}

	public ByteArray getPrimaryKey(ViewInfo leftView) {
		IndexColumn indexColumn = colNameToValue.get(leftView);
		return new ByteArray(indexColumn.getPrimaryKey());
	}

	public void mergeResults(IndexColumnInfo info) {
		for (Entry<ViewInfo, IndexColumn> entry : colNameToValue.entrySet()) {
			putIndexNode(entry.getKey(), entry.getValue());
		}
	}
	
}
