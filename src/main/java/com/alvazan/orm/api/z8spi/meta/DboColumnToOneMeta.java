package com.alvazan.orm.api.z8spi.meta;

import java.util.Collection;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;

@SuppressWarnings("rawtypes")
@NoSqlDiscriminatorColumn(value="fk")
public class DboColumnToOneMeta extends DboColumnMeta {
	
	/**
	 * This field may be referencing another entity in another table so here is the meta data
	 * on that table as well, but for now, I don't think we need it until we have joins
	 */
	@NoSqlManyToOne
	private DboTableMeta fkToColumnFamily;

	private boolean isPartitionedByThisColumn;

	public void setup(DboTableMeta owner, String colName, DboTableMeta fkToTable,
			boolean isIndexed, boolean isPartitionedBy) {
		super.setup(owner, colName, isIndexed);
		this.fkToColumnFamily = fkToTable;
		this.isPartitionedByThisColumn = isPartitionedBy;
	}

	public boolean isPartitionedByThisColumn() {
		return isPartitionedByThisColumn;
	}

	@Override
	public String getIndexTableName() {
		return getStorageType().getIndexTableName();
	}

	public DboTableMeta getFkToColumnFamily() {
		return fkToColumnFamily;
	}

	@Override
	public Class getClassType() {
		return fkToColumnFamily.getIdColumnMeta().getClassType();
	}

	@Override
	public StorageTypeEnum getStorageType() {
		DboColumnIdMeta idMeta = fkToColumnFamily.getIdColumnMeta();
		return idMeta.getStorageType();
	}

	public void translateFromColumn(Row row, TypedRow entity) {
		String columnName = getColumnName();
		byte[] namePrefix = StandardConverters.convertToBytes(columnName);
		Collection<Column> columns = row.columnByPrefix(namePrefix);
		if (columns != null && !columns.isEmpty()) {
			Column column = columns.iterator().next();
			byte[] value = column.getValue();
			byte[] fullName = column.getName();
			//strip off the prefix to get the foreign key
			int pkLen = fullName.length-namePrefix.length;
			byte[] fk = new byte[pkLen];
			for(int i = namePrefix.length; i < fullName.length; i++) {
				fk[i-namePrefix.length] =  fullName[i];
			}
			entity.addColumn(this, fullName, namePrefix, fk, value, column.getTimestamp());
		}
		else {
		     //Check if the column exists in old way
			Column column = row.getColumn(getColumnNameAsBytes());
			if(column == null)
				return;
			byte[] value = column.getValue();
			entity.addColumn(this, getColumnNameAsBytes(), value, column.getTimestamp());
		} 
	}

	@Override
	public void translateToColumn(InfoForIndex<TypedRow> info) {
		TypedRow entity = info.getEntity();
		RowToPersist row = info.getRow();

		Column col = new Column();
		row.getColumns().add(col);

		TypedColumn column = entity.getColumn(getColumnName());
		if (column != null) {
			byte[] byteVal = convertToStorage2(column.getValue());
			byte[] prefix = StandardConverters.convertToBytes(getColumnName());

			byte[] pkData = byteVal;

			byte[] name = new byte[prefix.length + pkData.length];
			for(int i = 0; i < name.length; i++) {
				if(i < prefix.length)
					name[i] = prefix[i];
				else
					name[i] = pkData[i-prefix.length];
			}

			col.setName(name);
			Object primaryKey = column.getValue();
			addIndexInfo(info, primaryKey, byteVal);
			removeIndexInfo(info, primaryKey, byteVal);	
		} else {
			byte[] prefix = StandardConverters.convertToBytes(getColumnName());
			col.setName(prefix);
		}
	}

	@Override
	public String fetchColumnValueAsString(TypedRow row) {
		TypedColumn typedCol = row.getColumn(getColumnName());
		Object value = typedCol.getValue();
		return convertTypeToString(value);
	}
	
}
