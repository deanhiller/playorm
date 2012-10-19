package com.alvazan.orm.api.z8spi.meta;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlOneToMany;
import com.alvazan.orm.api.base.anno.NoSqlOneToOne;
import com.alvazan.orm.api.base.anno.NoSqlQueries;
import com.alvazan.orm.api.base.anno.NoSqlQuery;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;

@SuppressWarnings("rawtypes")
@NoSqlEntity
@NoSqlQueries({
	@NoSqlQuery(name="findAll", query="SELECT t FROM TABLE as t"),
	@NoSqlQuery(name="findLike", query="SELECT t FROM TABLE as t WHERE t.columnFamily >= :prefix and t.columnFamily < :modifiedPrefix")
})
public class DboTableMeta {

	private static final Logger log = LoggerFactory.getLogger(DboTableMeta.class);
	
	@NoSqlIndexed
	@NoSqlId(usegenerator=false)
	private String columnFamily;

	private String actualColFamily;
	
	/**
	 * This is only used by our index tables at this time I believe.
	 * 
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
	
	@NoSqlOneToMany(keyFieldForMap="columnName")
	private Map<String, DboColumnMeta> nameToField = new HashMap<String, DboColumnMeta>();
	@NoSqlOneToOne
	private DboColumnIdMeta idColumn;

	private String foreignKeyToExtensions;

	private transient List<DboColumnMeta> indexedColumnsCache;
	private transient List<DboColumnMeta> cacheOfPartitionedBy;
	private transient Random r = new Random();

	/**
	 * Some virtual column families are embeddable
	 */
	private boolean isEmbeddable;
	
	private static Class typedRowProxyClass;

	static final Pattern NAME_PATTERN;
	
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
		logClassLoaders(clazz);
		testInstanceCreation(clazz);
		
		typedRowProxyClass = clazz;
		
		NAME_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z_0-9]*");
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
	
	private static void logClassLoaders(Class clazz) {
		logClassLoader("[proxies loaded in this one]", clazz.getClassLoader());
		
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		logClassLoader("[context classloader]", cl);
		
		ClassLoader sysCl = ClassLoader.getSystemClassLoader();
		logClassLoader("[system classloader]", sysCl);
		
		ClassLoader playCl = DboTableMeta.class.getClassLoader();
		logClassLoader("[play jar in this classloader]", playCl);
		
		ClassLoader assistCl = Proxy.class.getClassLoader();
		logClassLoader("[javassist jar in this classloader]", assistCl);
	}
	
	private static void logClassLoader(String prefix, ClassLoader loader) {
		ClassLoader cl = loader;
		String logMsg = "classloader list={";
		while(cl != null) {
			logMsg+=cl+",";
			cl = cl.getParent();
		}
		log.info("classloaders that proxies class exists in="+logMsg+"}");
	}

	public String getRealColumnFamily() {
		if(actualColFamily != null)
			return actualColFamily;
		return columnFamily;
	}
	public String getColumnFamily() {
		return columnFamily;
	}

	public String getRealVirtual() {
		if(isVirtualCf())
			return columnFamily;
		return null;
	}
	
	public boolean isVirtualCf() {
		return actualColFamily != null;
	}
	
	public boolean isEmbeddable() {
		return isEmbeddable;
	}

	public void setup(String virtualCf, String cf, boolean isEmbeddable) {
		if(!NAME_PATTERN.matcher(cf).matches())
			throw new IllegalArgumentException("Table name must match regular expression='[a-zA-Z_][a-zA-Z_0-9\\-]*'");

		if(virtualCf != null) {
			actualColFamily = cf;
			columnFamily = virtualCf;
		} else {
			this.columnFamily = cf;
		}
		
		this.isEmbeddable = isEmbeddable;
	}
	
	public void setRowKeyMeta(DboColumnIdMeta idMeta) {
		this.idColumn = idMeta;
	}
	
	public void addColumnMeta(DboColumnMeta fieldDbo) {
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
		
		StorageTypeEnum storedType = colNamePrefixType.getStoredType();
		this.colNamePrefixType = storedType.getDbValue();
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

		for(DboColumnMeta col : nameToField.values()) {
			col.translateToColumn(info);
		}

		//Now, let's write the leftover data here...
		for(TypedColumn col : typedRow.getColumnsAsColl()) {
			DboColumnMeta colMeta = col.getColumnMeta();
			if(colMeta != null)
				continue;
			
			List<Column> columns = row.getColumns();
			Column c = new Column();
			c.setName(col.getNameRaw());
			c.setValue(col.getValueRaw());
			columns.add(c);
		}
		
		return row;
	}
	
	public List<IndexData> findIndexRemoves(NoSqlTypedRowProxy proxy, byte[] rowKey) {
		initCaches();
		
		TypedRow r = (TypedRow) proxy;
		Map<String, Object> fieldToValue = proxy.__getOriginalValues();
		List<PartitionTypeInfo> partTypes = formPartitionTypesList(r);
		InfoForIndex<TypedRow> info = new InfoForIndex<TypedRow>(r, null, getColumnFamily(), fieldToValue, partTypes);
		List<IndexData> indexRemoves = new ArrayList<IndexData>();
		idColumn.removingThisEntity(info, indexRemoves, rowKey);

		for(DboColumnMeta indexed : this.indexedColumnsCache) {
			indexed.removingThisEntity(info, indexRemoves, rowKey);
		}
		
		return indexRemoves;
	}
	
	private List<PartitionTypeInfo> formPartitionTypesList(TypedRow row) {
		initCaches();
		
		List<PartitionTypeInfo> partTypes = new ArrayList<PartitionTypeInfo>();
		for(DboColumnMeta m : cacheOfPartitionedBy) {
			String by = m.getColumnName();
			String value = m.fetchColumnValueAsString(row);
			partTypes.add(new PartitionTypeInfo(by, value, m));
		}
		
		if(partTypes.size() == 0) {
			//if the table is not partitioned, then we still need to create the one huge partition
			partTypes.add(new PartitionTypeInfo(null, null, null));
		}
		return partTypes;
	}

	public <T> KeyValue<TypedRow> translateFromRow(Row row) {
		TypedRow typedRow = convertIdToProxy(row, typedRowProxyClass);
		fillInInstance(row, typedRow);
		NoSqlTypedRowProxy temp = (NoSqlTypedRowProxy)typedRow;
		//mark initialized so it doesn't hit the database again and cache original values so if they change
		//values we know we need to update the indexes and such...
		temp.__cacheIndexedValues();
		
		KeyValue<TypedRow> keyVal = new KeyValue<TypedRow>();
		keyVal.setKey(typedRow.getRowKey());
		keyVal.setValue(typedRow);
		return keyVal;
	}

	@SuppressWarnings("unchecked")
	private TypedRow convertIdToProxy(Row row, Class typedRowProxyClass) {
		Proxy inst = (Proxy) ReflectionUtil.create(typedRowProxyClass);
		inst.setHandler(new NoSqlTypedRowProxyImpl(this));
		TypedRow r = (TypedRow) inst;
		r.setMeta(this);
		return r;
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
		
		for(Column c : row.getColumns()) {
			byte[] name = c.getName();
			String strName = StandardConverters.convertFromBytes(String.class, name);
			if(this.nameToField.get(strName) != null)
				continue;
			
			inst.addColumn(c.getName(), c.getValue(), c.getTimestamp());
		}
	}

	public List<DboColumnMeta> getIndexedColumns() {
		initCaches();
		return indexedColumnsCache;
	}

	public List<DboColumnMeta> getPartitionedColumns() {
		initCaches();
		return cacheOfPartitionedBy;
	}

	private void initCaches() {
		if(indexedColumnsCache != null)
			return;
		
		indexedColumnsCache = new ArrayList<DboColumnMeta>();
		for(DboColumnMeta meta : nameToField.values()) {
			if(meta.isIndexed())
				indexedColumnsCache.add(meta);
		}
		if(idColumn.isIndexed())
			indexedColumnsCache.add(idColumn);
			
		cacheOfPartitionedBy = new ArrayList<DboColumnMeta>();
		for(DboColumnMeta meta : nameToField.values()) {
			if(meta.isPartitionedByThisColumn())
				cacheOfPartitionedBy.add(meta);
		}
	}


	public DboColumnMeta getAnyIndex() {
		initCaches();
		if(indexedColumnsCache.size() == 0)
			throw new IllegalArgumentException("The table="+columnFamily+" has no columnes with indexes.  ie. no entity attributes had the @NoSqlIndexed annotation");
		
		//spread load over the index rows .....
		int index = r.nextInt(indexedColumnsCache.size());
		
		return indexedColumnsCache.get(index);
	}


	public List<String> getColumnNameList() {
		List<String> names = new ArrayList<String>();
		names.add(idColumn.getColumnName());
		for(DboColumnMeta m : getAllColumns()) {
			names.add(m.getColumnName());
		}
		return names;
	}

	public boolean hasIndexedField() {
		initCaches();
		if(indexedColumnsCache.size() > 0 || idColumn.isIndexed())
			return true;
		return false;
	}

}