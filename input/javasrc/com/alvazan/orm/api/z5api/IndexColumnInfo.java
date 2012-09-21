package com.alvazan.orm.api.z5api;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;

public class IndexColumnInfo {

	private Map<ViewInfo, Wrapper> colNameToValue = new HashMap<ViewInfo, Wrapper>();
	
	public void putIndexNode(ViewInfo viewInfo, IndexColumn indCol, DboColumnMeta colMeta) {
		this.colNameToValue.put(viewInfo, new Wrapper(indCol, colMeta));
	}

	public Wrapper getIndexNode(ViewInfo view) {
		return colNameToValue.get(view);
	}

	public ByteArray getPrimaryKey(ViewInfo leftView) {
		return new ByteArray(getPrimaryKeyRaw(leftView));
	}
	public byte[] getPrimaryKeyRaw(ViewInfo info) {
		Wrapper wrapper = colNameToValue.get(info);
		if(wrapper == null)
			return null;
		IndexColumn col = colNameToValue.get(info).getCol();
		return col.getPrimaryKey();
	}

	public void mergeResults(IndexColumnInfo info) {
		for (Entry<ViewInfo, Wrapper> entry : info.colNameToValue.entrySet()) {
			colNameToValue.put(entry.getKey(), entry.getValue());
		}
	}

	public IndexColumnInfo copy() {
		IndexColumnInfo info = new IndexColumnInfo();
		for (Entry<ViewInfo, Wrapper> entry : colNameToValue.entrySet()) {
			info.colNameToValue.put(entry.getKey(), entry.getValue());
		}
		return info;
	}
	
	public IndexPoint getKeyForView(ViewInfo view) {
		Wrapper col = colNameToValue.get(view);
		DboColumnIdMeta idMeta = view.getTableMeta().getIdColumnMeta();
		if(col == null) 
			return new IndexPoint(idMeta, null, null);
		return new IndexPoint(idMeta, col.getCol(), col.getColMeta());
	}
	
	public static class Wrapper {
		private IndexColumn col;
		private DboColumnMeta colMeta;
		public Wrapper(IndexColumn col, DboColumnMeta colMeta) {
			super();
			this.col = col;
			this.colMeta = colMeta;
		}
		public IndexColumn getCol() {
			return col;
		}
		public DboColumnMeta getColMeta() {
			return colMeta;
		}
	}
}
