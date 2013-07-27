package com.alvazan.orm.api.z8spi.meta;

import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;

public class DboColumnMarkerMeta extends DboColumnMeta {

	@Override
	public boolean isPartitionedByThisColumn() {
		return false;
	}

	@Override
	public String getIndexTableName() {
		throw new UnsupportedOperationException("Indexing marker column is not supported");
	}

	@Override
	public Class getClassType() {
		return byte[].class;
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.NULL;
	}

	@Override
	public void translateToColumn(InfoForIndex<TypedRow> info) {
		// TODO Auto-generated method stub

	}

	@Override
	public void translateFromColumn(Row row, TypedRow inst) {
		// TODO Auto-generated method stub

	}

	@Override
	public String fetchColumnValueAsString(TypedRow row) {
		return "";
	}
}
