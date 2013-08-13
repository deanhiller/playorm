package com.alvazan.orm.api.z8spi.meta;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;

@NoSqlDiscriminatorColumn(value="ttl")
public class DboColumnTTLMeta extends DboColumnMeta {

	@Override
	public void setup(DboTableMeta owner2, String colName, boolean indexed) {
		if ( indexed) 
			throw new UnsupportedOperationException("Index on TTL column is not supported");
		super.setup(owner2, colName, indexed);
	}

	@Override
	public boolean isPartitionedByThisColumn() {
		/** TTL is meta column, can't be used for partitioning */
		return false;
	}

	@Override
	public String getIndexTableName() {
		throw new UnsupportedOperationException("Indexing TTL column is not supported");
	}

	@Override
	public Class<?> getClassType() {
		return Integer.class;
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.INTEGER;
	}

	@Override
	public void translateToColumn(InfoForIndex<TypedRow> info) {
		TypedColumn tcol = info.getEntity().getColumn(getColumnNameAsBytes());
		if (tcol != null) {
			info.getRow().setTtl(tcol.getValue(Integer.class));
		}
	}

	@Override
	public void translateFromColumn(Row row, TypedRow entity) {
		if (row.getColumns().isEmpty())
			return;

		Column column = row.getColumns().iterator().next();
		Integer ttl = column.getTtl();
		entity.addColumn(this, getColumnNameAsBytes(), ttl == null? new byte[4] : convertToStorage2(ttl), column.getTimestamp());
	}

	@Override
	public String fetchColumnValueAsString(TypedRow row) {
		TypedColumn tcol = row.getColumn(getColumnNameAsBytes());
		return tcol.getValue().toString();
	}
}
