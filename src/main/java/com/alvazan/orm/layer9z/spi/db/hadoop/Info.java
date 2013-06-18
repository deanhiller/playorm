package com.alvazan.orm.layer9z.spi.db.hadoop;

import org.apache.hadoop.hbase.HColumnDescriptor;

import com.alvazan.orm.api.z8spi.ColumnType;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;

public class Info {

	private StorageTypeEnum rowKeyType;
	private ColumnType columnType;
	private HColumnDescriptor colFamily;

	public HColumnDescriptor getColFamily() {
		return colFamily;
	}

	public void setColFamily(HColumnDescriptor colFamily) {
		this.colFamily = colFamily;
	}

	public ColumnType getColumnType() {
		return columnType;
	}

	public void setColumnType(ColumnType columnType) {
		this.columnType = columnType;
	}

	public StorageTypeEnum getRowKeyType() {
		return rowKeyType;
	}

	public void setRowKeyType(StorageTypeEnum rowKeyType) {
		this.rowKeyType = rowKeyType;
	}

}
