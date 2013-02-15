package com.alvazan.ssql.cmdline;

import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;

public class ColFamilyData {

	private String colFamily;
	private String column;
	private String partitionBy;
	private String partitionId;
	private DboTableMeta tableMeta;
	private DboColumnMeta columnMeta;

	public String getColFamily() {
		return colFamily;
	}
	public void setColFamily(String colFamily) {
		this.colFamily = colFamily;
	}
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
	public String getPartitionBy() {
		return partitionBy;
	}
	public void setPartitionBy(String partitionBy) {
		this.partitionBy = partitionBy;
	}
	public String getPartitionId() {
		return partitionId;
	}
	public void setPartitionId(String partitionId) {
		this.partitionId = partitionId;
	}
	public void setTableMeta(DboTableMeta meta) {
		this.tableMeta = meta;
	}
	public void setColumnMeta(DboColumnMeta colMeta) {
		this.columnMeta = colMeta;
	}
	public DboTableMeta getTableMeta() {
		return tableMeta;
	}
	public DboColumnMeta getColumnMeta() {
		return columnMeta;
	}
}
