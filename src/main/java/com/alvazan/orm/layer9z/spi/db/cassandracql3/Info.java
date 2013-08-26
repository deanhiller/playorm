package com.alvazan.orm.layer9z.spi.db.cassandracql3;

import com.alvazan.orm.api.z8spi.ColumnType;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;
import com.datastax.driver.core.TableMetadata;

public class Info {
	
	private StorageTypeEnum rowKeyType;
	private ColumnType columnType;
	private TableMetadata tableMetadata;
	public StorageTypeEnum getRowKeyType() {
		return rowKeyType;
	}
	public void setRowKeyType(StorageTypeEnum rowKeyType) {
		this.rowKeyType = rowKeyType;
	}
	public ColumnType getColumnType() {
		return columnType;
	}
	public void setColumnType(ColumnType columnType) {
		this.columnType = columnType;
	}
	public TableMetadata getTableMetadata() {
		return tableMetadata;
	}
	public void setTableMetadata(TableMetadata tableMetadata) {
		this.tableMetadata = tableMetadata;
	}
 
}
