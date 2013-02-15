package com.alvazan.orm.api.z8spi.action;

import com.alvazan.orm.api.z8spi.meta.DboTableMeta;




public class RemoveIndex implements Action {
	private DboTableMeta colFamily;
	private byte[] rowKey;
	private IndexColumn column;
	private String indexCfName;
	
	public DboTableMeta getColFamily() {
		return colFamily;
	}
	public void setColFamily(DboTableMeta indexColFamily) {
		this.colFamily = indexColFamily;
	}
	public byte[] getRowKey() {
		return rowKey;
	}
	public void setRowKey(byte[] rowKey) {
		this.rowKey = rowKey;
	}
	public IndexColumn getColumn() {
		return column;
	}
	public void setColumn(IndexColumn column) {
		this.column = column;
	}
	@Override
	public String toString() {
		return "indexChg to:"+colFamily.getColumnFamily()+"."+column.getColumnName();
	}
	public String getIndexCfName() {
		return indexCfName;
	}
	public void setIndexCfName(String indexCfName) {
		this.indexCfName = indexCfName;
	}
	
}
