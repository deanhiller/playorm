package com.alvazan.orm.api.spi1.meta;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlOneToMany;
import com.alvazan.orm.api.base.anno.NoSqlOneToOne;
import com.alvazan.orm.api.spi1.KeyValue;
import com.alvazan.orm.api.spi1.TypedRow;
import com.alvazan.orm.api.spi3.db.Row;

@SuppressWarnings("rawtypes")
@NoSqlEntity
public class DboTableMeta {

	@NoSqlId(usegenerator=false)
	private String columnFamily;

	/**
	 * A special case where the table has rows with names that are not Strings.  This is done frequently for indexes like
	 * indexes by time for instance where the name of the column might be a byte[] representing a long value or an int value
	 * In general, this is always a composite type of <indexed value type>.<primary key type> such that we can do a column
	 * scan on the indexed value type and then get the pk...the pk is part of the name because otherwise, it would not be unique
	 * and would collide with others that had the same indexed value.
	 */
	private String colNamePrefixType = null;
	/**
	 * This is the type of the column name which is nearly always a String (IT IS ALWAYS a string when usign the ORM layer).
	 */
	private String colNameType = String.class.getName();
	
	@NoSqlOneToMany(entityType=DboColumnMeta.class, keyFieldForMap="columnName")
	private Map<String, DboColumnMeta> nameToField = new HashMap<String, DboColumnMeta>();
	@NoSqlOneToOne
	private DboColumnIdMeta idColumn;

	private String foreignKeyToExtensions;

	private transient List<DboColumnMeta> indexedColumnsCache;
	private transient List<DboColumnMeta> cacheOfPartitionedBy;
	
	private static Class typedRowProxyClass;
	
	static {
		ProxyFactory f = new ProxyFactory();
		f.setSuperclass(TypedRow.class);
		f.setInterfaces(new Class[] {NoSqlTypedRowProxy.class});
		f.setFilter(new MethodFilter() {
			public boolean isHandled(Method m) {
				// ignore finalize()
				if(m.getName().equals("finalize"))
					return false;
				else if(m.getName().equals("equals"))
					return false;
				else if(m.getName().equals("hashCode"))
					return false;
				return true;
			}
		});
		Class clazz = f.createClass();
		testInstanceCreation(clazz);
		
		typedRowProxyClass = clazz;
	}
	
	private static Proxy testInstanceCreation(Class<?> clazz) {
		try {
			Proxy inst = (Proxy) clazz.newInstance();
			return inst;
		} catch (InstantiationException e) {
			throw new RuntimeException("Could not create proxy for type="+clazz, e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Could not create proxy for type="+clazz, e);
		}
	}
	
	
	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}
	
	void setRowKeyMeta(DboColumnIdMeta idMeta) {
		this.idColumn = idMeta;
	}
	
	void addColumnMeta(DboColumnMeta fieldDbo) {
		nameToField.put(fieldDbo.getColumnName(), fieldDbo);
	}
	
	public DboColumnMeta getColumnMeta(String columnName) {
		return nameToField.get(columnName);
	}

	public void setColNameType(Class c) {
		Class objType = DboColumnMeta.translateType(c);
		this.colNameType = objType.getName();
	}
	public StorageTypeEnum getNameStorageType() {
		Class clazz = DboColumnMeta.classForName(colNameType);
		return DboColumnMeta.getStorageType(clazz);
	}
//	
//	@SuppressWarnings("rawtypes")
//	public Class getColumnNameType() {
//		return DboColumnMeta.classForName(columnNameType);
//	}
	
	public StorageTypeEnum getColNamePrefixType() {
		return StorageTypeEnum.lookupValue(colNamePrefixType);
	}

	public void setColNamePrefixType(StorageTypeEnum colNamePrefixType) {
		if(colNamePrefixType == null) {
			this.colNamePrefixType = null;
			return;
		}
		this.colNamePrefixType = colNamePrefixType.getDbValue();
	}

	@Override
	public String toString() {
		return "[tablename="+columnFamily+" indexedcolumns="+nameToField.values()+" pk="+idColumn+"]";
	}

	public DboColumnIdMeta getIdColumnMeta() {
		return idColumn;
	}

	public String getForeignKeyToExtensions() {
		return foreignKeyToExtensions;
	}

	public void setForeignKeyToExtensions(String foreignKeyToExtensions) {
		this.foreignKeyToExtensions = foreignKeyToExtensions;
	}

	public Collection<DboColumnMeta> getAllColumns() {
		return nameToField.values();
	}

	public RowToPersist translateToRow(TypedRow typedRow) {
		RowToPersist row = new RowToPersist();
		Map<String, Object> fieldToValue = null;
		if(typedRow instanceof NoSqlTypedRowProxy) {
			fieldToValue = ((NoSqlTypedRowProxy) typedRow).__getOriginalValues();
		}
		
		List<PartitionTypeInfo> partTypes = formPartitionTypesList(typedRow);
		InfoForIndex<TypedRow> info = new InfoForIndex<TypedRow>(typedRow, row, getColumnFamily(), fieldToValue, partTypes);

		idColumn.translateToColumn(info);

		for(DboColumnMeta m : nameToField.values()) {
			m.translateToColumn(info);
		}
		
		return row;
	}
	

	
	private List<PartitionTypeInfo> formPartitionTypesList(TypedRow row) {
		initCaches();
		
		List<PartitionTypeInfo> partTypes = new ArrayList<PartitionTypeInfo>();
		for(DboColumnMeta m : cacheOfPartitionedBy) {
			String by = m.getColumnName();
			String value = m.fetchColumnValueAsString(row);
			partTypes.add(new PartitionTypeInfo(by, value));
		}
		
		if(partTypes.size() == 0) {
			//if the table is not partitioned, then we still need to create the one huge partition
			partTypes.add(new PartitionTypeInfo(null, null));
		}
		return partTypes;
	}

	public <T> KeyValue<TypedRow<T>> translateFromRow(Row row) {
		TypedRow typedRow = convertIdToProxy(row, row.getKey(), typedRowProxyClass);
		fillInInstance(row, typedRow);
		NoSqlTypedRowProxy temp = (NoSqlTypedRowProxy)typedRow;
		//mark initialized so it doesn't hit the database again and cache original values so if they change
		//values we know we need to update the indexes and such...
		temp.__cacheIndexedValues();
		
		KeyValue<TypedRow<T>> keyVal = new KeyValue<TypedRow<T>>();
		keyVal.setKey(typedRow.getRowKey());
		keyVal.setValue(typedRow);
		return keyVal;
	}

	@SuppressWarnings("unchecked")
	private TypedRow convertIdToProxy(Row row, byte[] key, Class typedRowProxyClass) {
		Proxy inst = (Proxy) ReflectionUtil.create(typedRowProxyClass);
		inst.setHandler(new NoSqlTypedRowProxyImpl(this));
		return (TypedRow) inst;
	}

	private TypedRow createInstance(Class<? extends TypedRow> typedRowProxyClass) {
		try {
			return typedRowProxyClass.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * @param row
	 * @param session - The session to pass to newly created proxy objects
	 * @param inst The object OR the proxy to be filled in
	 * @return The key of the entity object
	 */
	public void fillInInstance(Row row, TypedRow inst) {
		idColumn.translateFromColumn(row, inst);

		for(DboColumnMeta column : this.nameToField.values()) {
			column.translateFromColumn(row, inst);
		}
	}

	public List<DboColumnMeta> getIndexedColumns() {
		initCaches();
		return indexedColumnsCache;
	}


	private void initCaches() {
		if(indexedColumnsCache != null)
			return;
		
		indexedColumnsCache = new ArrayList<DboColumnMeta>();
		for(DboColumnMeta meta : nameToField.values()) {
			if(meta.isIndexed())
				indexedColumnsCache.add(meta);
		}
			
		cacheOfPartitionedBy = new ArrayList<DboColumnMeta>();
		for(DboColumnMeta meta : nameToField.values()) {
			if(meta.isPartitionedByThisColumn())
				cacheOfPartitionedBy.add(meta);
		}
	}
	
}