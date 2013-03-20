package com.alvazan.orm.layer9z.spi.db.mongodb;

import com.alvazan.orm.api.z8spi.ColumnType;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;
import com.mongodb.DBCollection;

public class Info {

	private StorageTypeEnum rowKeyType;
	private ColumnType columnType;
	private DBCollection columnFamilyObj;
	
	public ColumnType getColumnType() {
		return columnType;
	}

	public void setColumnType(ColumnType columnType) {
		this.columnType = columnType;
	}

	public void setColumnFamilyObj(DBCollection cf) {
		this.columnFamilyObj = cf;
	}


	public DBCollection getColumnFamilyObj() {
		return columnFamilyObj;
	}

	public StorageTypeEnum getRowKeyType() {
		return rowKeyType;
	}

	public void setRowKeyType(StorageTypeEnum rowKeyType) {
		this.rowKeyType = rowKeyType;
	}

}
