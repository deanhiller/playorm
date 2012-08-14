package com.alvazan.orm.api.spi2.meta;

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
import com.alvazan.orm.api.spi2.InfoForIndex;
import com.alvazan.orm.api.spi2.KeyValue;
import com.alvazan.orm.api.spi2.NoSqlTypedRowProxy;
import com.alvazan.orm.api.spi2.NoSqlTypedRowProxyImpl;
import com.alvazan.orm.api.spi2.ReflectionUtil;
import com.alvazan.orm.api.spi2.RowToPersist;
import com.alvazan.orm.api.spi2.TypedRow;
import com.alvazan.orm.api.spi3.db.Row;

@SuppressWarnings("rawtypes")
@NoSqlEntity
public class DboTableMeta {

	@NoSqlId(usegenerator=false)
	private String columnFamily;
	
	private String colNamePrefixType = null;
	private String colNameType = String.class.getName();
//	/**
//	 * A special case where the table has rows with names that are not Strings.  This is done frequently for indexes like
//	 * indexes by time for instance where the name of the column might be a byte[] representing a long value or an int value
//	 */
//	private String columnNameType = String.class.getName();
//	private String 
//	private String valueType = void.class.getName();
	
	@NoSqlOneToMany(entityType=DboColumnMeta.class, keyFieldForMap="columnName")
	private Map<String, DboColumnMeta> nameToField = new HashMap<String, DboColumnMeta>();
	@NoSqlOneToOne
	private DboColumnIdMeta idColumn;

	private String foreignKeyToExtensions;

	private transient List<DboColumnMeta> indexedColumnsCache;
	
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
		
		InfoForIndex<TypedRow> info = new InfoForIndex<TypedRow>(typedRow, row, getColumnFamily(), fieldToValue);

		idColumn.translateToColumn(info);

		for(DboColumnMeta m : nameToField.values()) {
			m.translateToColumn(info);
		}
		
		return row;
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
		if(indexedColumnsCache == null) {
			indexedColumnsCache = new ArrayList<DboColumnMeta>();
			for(DboColumnMeta meta : nameToField.values()) {
				if(meta.isIndexed())
					indexedColumnsCache.add(meta);
			}
		}
		return indexedColumnsCache;
	}
	
}