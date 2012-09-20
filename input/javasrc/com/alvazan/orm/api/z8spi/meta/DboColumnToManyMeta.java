package com.alvazan.orm.api.z8spi.meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;

@SuppressWarnings("rawtypes")
@NoSqlDiscriminatorColumn(value="listOfFk")
public class DboColumnToManyMeta extends DboColumnMeta {

	/**
	 * This field may be referencing another entity in another table so here is the meta data
	 * on that table as well, but for now, I don't think we need it until we have joins
	 */
	@NoSqlManyToOne
	private DboTableMeta fkToColumnFamily;

	/**
	 * When someone in an entity uses CursorToMany, it is because there are too many and could blow out the memory so
	 * they use CursorToMany and we store the tomany in a separate index row so they can cursor over the results.
	 */
	private Boolean isSeparateRow;
	
	public void setup(DboTableMeta owner, String colName, DboTableMeta fkToTable, boolean isCursor) {
		super.setup(owner, colName, false);
		this.fkToColumnFamily = fkToTable;
		this.isSeparateRow = isCursor;
	}
	
	@Override
	public String getIndexTableName() {
		StorageTypeEnum storageType = fkToColumnFamily.getIdColumnMeta().getStorageType();
		return storageType.getIndexTableName();
	}
	
	public DboTableMeta getFkToColumnFamily() {
		return fkToColumnFamily;
	}

	public boolean isSeparateRow() {
		if(isSeparateRow = null)
			return false;
		return isSeparateRow;
	}
	
	@Override
	public Class getClassType() {
		throw new UnsupportedOperationException("Need to figure out how to convert Class to a type of Array Class");
//		Class typeInTheArray = fkToColumnFamily.getIdColumnMeta().getClassType();
	}

	@Override
	public StorageTypeEnum getStorageType() {
		throw new UnsupportedOperationException("Need to figure out how to convert Class to a type of Array Class");
//		StorageTypeEnum typeInTheArray = fkToColumnFamily.getIdColumnMeta().getStorageType();
	}

	@Override
	public void translateFromColumn(Row row, TypedRow entity) {
		List<Object> pks = translateFromColumnList(row, entity);
		entity.addColumn(getColumnName(), pks);
	}

	private List<Object> translateFromColumnList(Row row, TypedRow entity) {
		List<byte[]> keys = parseOutKeyList(row);
		List<Object> fks = new ArrayList<Object>();
		for(byte[] key : keys) {
			Object fk = convertFromStorage2(key);
			fks.add(fk);
		}

		return fks;
	}

	private List<byte[]> parseOutKeyList(Row row) {
		String columnName = getColumnName();
		byte[] bytes = StandardConverters.convertToBytes(columnName);
		Collection<Column> columns = row.columnByPrefix(bytes);
		List<byte[]> entities = new ArrayList<byte[]>();

		//NOTE: Current implementation is just like a Set not a List in that it
		//cannot have repeats right now.  We could take the approach the column name
		//would be <prefix><index><pk> such that duplicates are allowed and when loaded
		//we would have to keep the index in the proxy so if removed, we could put the col name
		//back together so we could remove it and add the new one.  This is very complex though when 
		//it comes to removing one item and shifting all other column names by one(ie. lots of removes/adds)
		//so for now, just make everything Set like.
		for(Column col : columns) {
			byte[] colNameData = col.getName();
			//strip off the prefix to get the foreign key
			int pkLen = colNameData.length-bytes.length;
			byte[] pk = new byte[pkLen];
			for(int i = bytes.length; i < colNameData.length; i++) {
				pk[i-bytes.length] =  colNameData[i];
			}
			entities.add(pk);
		}
		
		return entities;
	}
	
	@Override
	public void translateToColumn(InfoForIndex<TypedRow> info) {
		TypedRow entity = info.getEntity();
		RowToPersist row = info.getRow();
		translateToColumnList(entity, row);
	}

	@SuppressWarnings({ "unchecked" })
	private void translateToColumnList(TypedRow entity, RowToPersist row) {
		TypedColumn column = entity.getColumn(getColumnName());
		Object valueObj = column.getValue();
		if(!(valueObj instanceof Collection))
			throw new IllegalArgumentException("For column family="+this.owner.getColumnFamily()+" you passed in a row and column value for column="+getColumnName()+" must inherit from Collection and does not(for this column as it is a ToMany column)");
		Collection fks = (Collection) valueObj;
		
		Collection<Object> toBeAdded = fks; //all values in the list get added if not an OurAbstractCollection
		Collection<Object> toBeRemoved = new ArrayList<Object>();
		
		throw new UnsupportedOperationException("not complete yet, need to add the below code but use a more simple list");
//		if(fks instanceof OurAbstractCollection) {
//			OurAbstractCollection<Object> coll = (OurAbstractCollection<Object>)fks;
//			toBeRemoved = coll.getToBeRemoved();
//			toBeAdded = coll.getToBeAdded();
//		}
//		
//		translateToColumnImpl(toBeAdded, row, toBeRemoved);
	}

	private void translateToColumnImpl(Collection<Object> toBeAdded, RowToPersist row, Collection<Object> toBeRemoved) {
		//removes first
		for(Object theFk : toBeRemoved) {
			byte[] name = formTheNameImpl(theFk);
			row.addEntityToRemove(name);
		}
		
		//now process all the existing columns (we can add same entity as many times as we like and it does not
		//get duplicated)
		for(Object fk : toBeAdded) {
			byte[] name = formTheNameImpl(fk);
			Column c = new Column();
			c.setName(name);
			row.getColumns().add(c);
		}
	}
	
	private byte[] formTheNameImpl(Object p) {
		byte[] byteVal = converter.convertToNoSql(p);
		byte[] pkData = byteVal;
		
		byte[] prefix = getColumnNameAsBytes();
		byte[] name = new byte[prefix.length + pkData.length];
		for(int i = 0; i < name.length; i++) {
			if(i < prefix.length)
				name[i] = prefix[i];
			else
				name[i] = pkData[i-prefix.length];
		}
		return name;
	}

	@Override
	public boolean isPartitionedByThisColumn() {
		return false;
	}	
	@Override
	public String fetchColumnValueAsString(TypedRow row) {
		throw new UnsupportedOperationException("only used by partitioning and can't partition a multe value column....that is kind of useless");
	}
}
