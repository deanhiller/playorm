package com.alvazan.orm.api.spi.layer2;

import java.util.HashMap;
import java.util.Map;

import com.alvazan.orm.api.base.anno.Id;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.OneToMany;
import com.alvazan.orm.api.base.anno.OneToOne;

@NoSqlEntity
public class DboTableMeta {

	@Id(usegenerator=false)
	private String columnFamily;
	
	/**
	 * A special case where the table has rows with names that are not Strings.  This is done frequently for indexes like
	 * indexes by time for instance where the name of the column might be a byte[] representing a long value or an int value
	 */
	private String columnNameType = String.class.getName();
	
	@OneToMany(entityType=DboColumnMeta.class, keyFieldForMap="columnName")
	private Map<String, DboColumnMeta> nameToField = new HashMap<String, DboColumnMeta>();
	@OneToOne
	private DboColumnMeta idColumn;
	
	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}
	
	public void setRowKeyMeta(DboColumnMeta idMeta) {
		this.idColumn = idMeta;
	}
	
	public Map<String, DboColumnMeta> getColumns() {
		return nameToField;
	}
	
	public void addColumnMeta(DboColumnMeta fieldDbo) {
		nameToField.put(fieldDbo.getColumnName(), fieldDbo);
	}
	
	public DboColumnMeta getColumnMeta(String columnName) {
		return nameToField.get(columnName);
	}
	
	@SuppressWarnings("rawtypes")
	public void setColumnNameType(Class c) {
		Class objType = DboColumnMeta.translateType(c);
		this.columnNameType = objType.getName();
	}
	
	@SuppressWarnings("rawtypes")
	public Class getColumnNameType() {
		return DboColumnMeta.classForName(columnNameType);
	}
	
	@Override
	public String toString() {
		return "[tablename="+columnFamily+" indexedcolumns="+nameToField.values()+" pk="+idColumn+"]";
	}

	public DboColumnMeta getIdColumnMeta() {
		return idColumn;
	}

}