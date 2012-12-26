package com.alvazan.orm.api.z8spi.meta;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import java.util.Collection;

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
		DboColumnIdMeta idMeta = fkToColumnFamily.getIdColumnMeta();
		StorageTypeEnum storageType = idMeta.getStorageType();
		return storageType.getIndexTableName();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class getClassType() {
		return null;
	}

	@Override
	public StorageTypeEnum getStorageType() {
		StorageTypeEnum typeInTheArray = fkToColumnFamily.getIdColumnMeta().getStorageType();
		return typeInTheArray;
	}

	@Override
	public void translateToColumn(InfoForIndex<TypedRow> info) {

	}

	@Override
	public void translateFromColumn(Row row, TypedRow inst) {
		translateFromColumnList(row, inst);
	}

	private void translateFromColumnList(Row row, TypedRow entity) {
		parseOutKeyList(row, entity);
	}

	private void parseOutKeyList(Row row, TypedRow entity) {
		String columnName = getColumnName();
		byte[] namePrefix = StandardConverters.convertToBytes(columnName);
		Collection<Column> columns = row.columnByPrefix(namePrefix);
		for(Column col : columns) {
			byte[] fullName = col.getName();
			int pkLen = fullName.length-namePrefix.length;
			byte[] fk = new byte[pkLen];
			for(int i = namePrefix.length; i < fullName.length; i++) {
				fk[i-namePrefix.length] =  fullName[i];
			}
			entity.addColumn(this, fullName, namePrefix, fk, col.getValue(), col.getTimestamp());
		}
	}

	@Override
	public String fetchColumnValueAsString(TypedRow row) {
		return null;
	}

}
