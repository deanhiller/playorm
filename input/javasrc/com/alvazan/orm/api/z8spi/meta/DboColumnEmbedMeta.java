package com.alvazan.orm.api.z8spi.meta;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;

@NoSqlDiscriminatorColumn(value="embedded")
public class DboColumnEmbedMeta extends DboColumnMeta {

	@NoSqlManyToOne
	private DboTableMeta fkToColumnFamily;
	
	public void setup(DboTableMeta t, String colName, DboTableMeta fkToTable) {
		super.setup(t, colName, false);
		this.fkToColumnFamily = fkToTable;
	}
	
	@Override
	public boolean isPartitionedByThisColumn() {
		return false;
	}

	@Override
	public String getIndexTableName() {
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class getClassType() {
		return null;
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return null;
	}

	@Override
	public void translateToColumn(InfoForIndex<TypedRow> info) {

	}

	@Override
	public void translateFromColumn(Row row, TypedRow inst) {

	}

	@Override
	public String fetchColumnValueAsString(TypedRow row) {
		return null;
	}

}
