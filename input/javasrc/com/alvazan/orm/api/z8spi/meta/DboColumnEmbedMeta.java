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
		String columnName = getColumnName() + "Id";
		byte[] namePrefix = StandardConverters.convertToBytes(columnName);
		Collection<Column> columns = row.columnByPrefix(namePrefix);
		for(Column col : columns) {
			byte[] rowkeyFullName = col.getName();
			int rkLen = rowkeyFullName.length-namePrefix.length;
			byte[] rk = new byte[rkLen];
			for(int i = namePrefix.length; i < rowkeyFullName.length; i++) {
				rk[i-namePrefix.length] =  rowkeyFullName[i];
			}
			//this is the rowkey which is being added
			String rowKeyToDisplay = getColumnName() + ".Id";
			byte[] rowkeyPrefixToDisplay = StandardConverters.convertToBytes(rowKeyToDisplay);
			entity.addColumn(this, rowkeyFullName, rowkeyPrefixToDisplay, rk, col.getValue(), col.getTimestamp());

			//Now extract other columns
			Object objVal = this.convertFromStorage2(rk);
			String columnsInEmbeddedRowName = getColumnName() + this.convertTypeToString(objVal);
			byte[] embedColumn = StandardConverters.convertToBytes(columnsInEmbeddedRowName);
			Collection<Column> columnsInRow = row.columnByPrefix(embedColumn);
			for(Column colInRow : columnsInRow) {
				byte[] fullName = colInRow.getName();
				int pkLen = fullName.length-embedColumn.length;
				byte[] fk = new byte[pkLen];
				for(int i = embedColumn.length; i < fullName.length; i++) {
					fk[i-embedColumn.length] =  fullName[i];
				}

				String embedColumnToDisplay = getColumnName() + "." + this.convertTypeToString(objVal);
				byte[] embedColumPrefixToDisplay = StandardConverters.convertToBytes(embedColumnToDisplay);
				entity.addColumn(this, fullName, embedColumPrefixToDisplay, fk, colInRow.getValue(), col.getTimestamp());
			}
		}
	}

	@Override
	public String fetchColumnValueAsString(TypedRow row) {
		return null;
	}

}
