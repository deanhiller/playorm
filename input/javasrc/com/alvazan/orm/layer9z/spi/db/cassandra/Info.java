package com.alvazan.orm.layer9z.spi.db.cassandra;

import com.alvazan.orm.api.z8spi.ColumnType;
import com.alvazan.orm.api.z8spi.meta.StorageTypeEnum;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;

@SuppressWarnings("rawtypes")
public class Info {

	private StorageTypeEnum rowKeyType;
	private ColumnType columnType;
	private AnnotatedCompositeSerializer compositeSerializer;
	private ColumnFamily columnFamilyObj;

	public ColumnType getColumnType() {
		return columnType;
	}

	public void setColumnType(ColumnType columnType) {
		this.columnType = columnType;
	}

	public void setCompositeSerializer(
			AnnotatedCompositeSerializer bigIntSer) {
		this.compositeSerializer = bigIntSer;
	}

	public void setColumnFamilyObj(ColumnFamily cf) {
		this.columnFamilyObj = cf;
	}

	public AnnotatedCompositeSerializer getCompositeSerializer() {
		return compositeSerializer;
	}

	public ColumnFamily getColumnFamilyObj() {
		return columnFamilyObj;
	}

	public StorageTypeEnum getRowKeyType() {
		return rowKeyType;
	}

	public void setRowKeyType(StorageTypeEnum rowKeyType) {
		this.rowKeyType = rowKeyType;
	}
	
}
