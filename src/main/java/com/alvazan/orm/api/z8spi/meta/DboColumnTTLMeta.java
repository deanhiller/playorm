package com.alvazan.orm.api.z8spi.meta;

import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;

public class DboColumnTTLMeta extends DboColumnMeta {

	@Override
	public boolean isPartitionedByThisColumn() {
		/** TTL is metacolumn, can't be used for partitioning */
		return false;
	}

	@Override
	public String getIndexTableName() {
		throw new UnsupportedOperationException("Indexing TTL column is not supported");
	}

	@Override
	public Class getClassType() {
		return Integer.class;
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.INTEGER;
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
		// TODO Auto-generated method stub
		return null;
	}
}
