package com.alvazan.orm.api.z8spi.meta;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;

@SuppressWarnings("rawtypes")
@NoSqlDiscriminatorColumn(value="generic")
public class DboColumnCommonMeta extends DboColumnMeta {

	private String columnValueType;
	private boolean isPartitionedByThisColumn;
	
	public void setup(DboTableMeta owner, String colName, Class valuesType, boolean isIndexed, boolean isPartitionedBy) {
		super.setup(owner, colName, isIndexed);
		Class newType = ConverterUtil.translateType(valuesType);
		this.columnValueType = newType.getName();
		this.isPartitionedByThisColumn = isPartitionedBy;
	}
	
	public boolean isPartitionedByThisColumn() {
		return isPartitionedByThisColumn;
	}
	
	@Override
	public String getIndexTableName() {
		StorageTypeEnum type = getStorageType();
		if(type == null)
			throw new IllegalArgumentException(getClassType()+" is not supported at this time");
		return getStorageType().getIndexTableName();
	}

	public Class getClassType() {
		return ConverterUtil.classForName(columnValueType);
	}

	public StorageTypeEnum getStorageType() {
		Class fieldType = getClassType();
		return ConverterUtil.getStorageType(fieldType);
	}

	public void translateFromColumn(Row row, TypedRow entity) {
		Column column = row.getColumn(getColumnNameAsBytes());
		if(column == null) {
			return;
		}

		Long timestamp = column.getTimestamp();
		entity.addColumn(this, getColumnNameAsBytes(), column.getValue(), timestamp);
	}
	
	public void translateToColumn(InfoForIndex<TypedRow> info) {
		TypedRow typedRow = info.getEntity();
		RowToPersist row = info.getRow();

		TypedColumn typedCol = typedRow.getColumn(getColumnName());
		Object value = null;
		byte[] byteVal = null;
		if(typedCol != null) {
			Column col = new Column();
			row.getColumns().add(col);
			
			value = typedCol.getValue();
			byteVal = convertToStorage2(value);
			col.setName(getColumnNameAsBytes());
			col.setValue(byteVal);
		}		
		addIndexInfo(info, value, byteVal);
		removeIndexInfo(info, value, byteVal);
	}

	@Override
	public String fetchColumnValueAsString(TypedRow row) {
		TypedColumn typedCol = row.getColumn(getColumnName());
		Object value = typedCol.getValue();
		return convertTypeToString(value);
	}

}
