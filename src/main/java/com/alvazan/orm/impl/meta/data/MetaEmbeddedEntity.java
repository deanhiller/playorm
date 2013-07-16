package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alvazan.orm.api.base.ToOneProvider;
import com.alvazan.orm.api.base.anno.NoSqlConverter;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlTransient;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.meta.DboColumnEmbedMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.RowToPersist;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.Converter;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.meta.InfoForIndex;
import com.alvazan.orm.api.z8spi.meta.ReflectionUtil;
import com.alvazan.orm.impl.meta.data.collections.MapProxyFetchAll;
import com.alvazan.orm.impl.meta.data.collections.SetProxyFetchAll;
import com.alvazan.orm.impl.meta.data.collections.SimpleAbstractCollection;
import com.alvazan.orm.impl.meta.data.collections.SimpleList;
import com.alvazan.orm.impl.meta.data.collections.ToOneProviderProxy;

public class MetaEmbeddedEntity<OWNER, PROXY> extends MetaAbstractField<OWNER> {

	private DboColumnEmbedMeta metaDbo = new DboColumnEmbedMeta();
	private MetaAbstractClass<PROXY> classMeta;
	private Field fieldForKey;

	@Override
	public String toString() {
		return "MetaEmbeddedEntity [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType()+ "), columnName=" + columnName + "]";
	}

	public void setup(DboTableMeta t, Field field, String colName,
			MetaAbstractClass<PROXY> fkMeta) {
		DboTableMeta fkToTable = fkMeta.getMetaDbo();
		metaDbo.setup(t, colName, fkToTable);
		super.setup(field, colName);
		this.classMeta = fkMeta;
	}

	@Override
	public void translateFromColumn(Row row, OWNER entity, NoSqlSession session) {
		Object proxy;
		if (field.getType().equals(Map.class))
			proxy = translateFromColumnMap(row, entity, session);
		else if (field.getType().equals(Collection.class)
				|| field.getType().equals(List.class))
			proxy = translateFromColumnList(row, entity, session);
		else if (field.getType().equals(Set.class))
			proxy = translateFromColumnSet(row, entity, session);
		else {
			proxy = translateFromSingleEntity(row, session);
		}

		ReflectionUtil.putFieldValue(entity, field, proxy);
	}

	private Object translateFromSingleEntity(Row row, NoSqlSession session) {
		Object proxy = null;
		String columnName = getColumnName();
		byte[] colBytes = StandardConverters.convertToBytes(columnName);
		Column column = row.getColumn(colBytes);
		if (column == null) {
			column = new Column();
		}
		if (field.getType().equals(ToOneProvider.class)) {
			// THIS IS NOT DONE YET
			proxy = translateFromToComposite(row, session);
		} else {
				proxy = convertIdToProxyComposite(row, session);
		}
		return proxy;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Object translateFromToComposite(Row row, NoSqlSession session) {
		// THIS IS NOT DONE YET
		byte[] bytes = StandardConverters.convertToBytes(columnName);
		Collection<Column> columns = row.columnByPrefix(bytes);
		if (columns != null && !columns.isEmpty()) {
			Column column = columns.iterator().next();
			byte[] fullName = column.getName();
			// strip off the prefix to get the foreign key
			int pkLen = fullName.length - bytes.length;
			byte[] fk = new byte[pkLen];
			for (int i = bytes.length; i < fullName.length; i++) {
				fk[i - bytes.length] = fullName[i];
			}
			ToOneProvider<PROXY> toOne = new ToOneProviderProxy(classMeta, fk,
					session);
			return toOne;
		} else
			return null;
	}

	private Object convertIdToProxyComposite(Row row, NoSqlSession session) {
		byte[] bytes = StandardConverters.convertToBytes(columnName);
		byte[] rowid = StandardConverters.convertToBytes("Id");
        int bytesandrowid = bytes.length + rowid.length;
        byte[] bytesandId = new byte[bytesandrowid];
        System.arraycopy(bytes, 0, bytesandId, 0, bytes.length);
        System.arraycopy(rowid, 0, bytesandId, bytes.length, rowid.length);
        Collection<Column> columns = row.columnByPrefix(bytesandId);
        if (columns != null && !columns.isEmpty()) {
            // Id is present
            Column column = columns.iterator().next();
            byte[] fullName = column.getName();
            // strip off the prefix to get the foreign key
            int pkLen = fullName.length - bytesandrowid;
            byte[] pk = new byte[pkLen];
            for (int i = bytesandrowid; i < fullName.length; i++) {
                pk[i - bytesandrowid] = fullName[i];
            }
            return createProxy(pk, row);
        } else
            return createProxy(null, row);
    }

	private Object translateFromColumnSet(Row row, OWNER entity,
			NoSqlSession session) {
		List<byte[]> keys = parseColNamePostfix(columnName, row);
		Set<PROXY> retVal = new SetProxyFetchAll<PROXY>(entity, session,
				classMeta, keys, field);
		return retVal;
	}

	@SuppressWarnings({ "rawtypes" })
	private Map translateFromColumnMap(Row row, OWNER entity,
			NoSqlSession session) {
		List<byte[]> keys = parseColNamePostfix(columnName, row);
		MapProxyFetchAll proxy = MapProxyFetchAll.create(entity, session,
				classMeta, keys, fieldForKey, field);
		return proxy;
	}

	@SuppressWarnings("unchecked")
	private List<PROXY> translateFromColumnList(Row row, OWNER entity,
			NoSqlSession session) {
		List<byte[]> keys = parseColNamePostfix(columnName, row);
		List<PROXY> retVal = new ArrayList<PROXY>();
		for (byte[] rowkey : keys) {
			Object proxy = createProxy(rowkey, row);
			retVal.add((PROXY)proxy);
		}
		List<PROXY> finalList = new SimpleList<PROXY>(retVal);
		return finalList;
	}

	@Override
	public void translateToColumn(InfoForIndex<OWNER> info) {
		OWNER entity = info.getEntity();
		RowToPersist row = info.getRow();
		if (field.getType().equals(Map.class))
			translateToColumnMap(entity, row);
		else if (field.getType().equals(List.class))
			translateToColumnList(entity, row);
		else translateToColumn(entity, row);
	}

	@SuppressWarnings("unchecked")
	private void translateToColumn(OWNER entity, RowToPersist row) {
		Collection<PROXY> value = new ArrayList<PROXY>();
		value.add((PROXY)ReflectionUtil.fetchFieldValue(entity, field));
		Collection<PROXY> toBeRemoved = new ArrayList<PROXY>();
		translateToColumnImpl(value, row, toBeRemoved);
	}

	@SuppressWarnings("unchecked")
	private void translateToColumnList(OWNER entity, RowToPersist row) {
		Collection<PROXY> values = (Collection<PROXY>) ReflectionUtil
				.fetchFieldValue(entity, field);
		Collection<PROXY> toBeAdded = values;
		// all values in the list get
		// added if not an
		// SimpleAbstractCollection
		Collection<PROXY> toBeRemoved = new ArrayList<PROXY>();
		if (values instanceof SimpleAbstractCollection) {
		    SimpleAbstractCollection<PROXY> coll = (SimpleAbstractCollection<PROXY>) values;
			toBeRemoved = coll.getToBeRemoved();
			toBeAdded = coll.getToBeAdded();
		}
		translateToColumnImpl(toBeAdded, row, toBeRemoved);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void translateToColumnMap(OWNER entity, RowToPersist row) {
		Map mapOfProxies = (Map) ReflectionUtil.fetchFieldValue(entity, field);
		Collection<PROXY> toBeAdded = mapOfProxies.values();
		Collection<PROXY> toBeRemoved = new ArrayList<PROXY>();
		if (mapOfProxies instanceof MapProxyFetchAll) {
			MapProxyFetchAll mapProxy = (MapProxyFetchAll) mapOfProxies;
			toBeRemoved = mapProxy.getToBeRemoved();
			toBeAdded = mapProxy.getToBeAdded();
		}
		translateToColumnImpl(toBeAdded, row, toBeRemoved);
	}

	private void translateToColumnImpl(Collection<PROXY> toBeAdded,
			RowToPersist row, Collection<PROXY> toBeRemoved) {
		// removes first
		for (PROXY p : toBeRemoved) {
			byte[] name = formTheName(p);
			row.addEntityToRemove(name);
		}
		if (toBeAdded == null)
			return;
		for (PROXY proxy : toBeAdded) {
			byte[] name = formTheName(proxy);
			if (name != null) {
				// rowkey is not null. i.e., NoSqlId is present
				Column idColumn = new Column();
				idColumn.setName(name);
				idColumn.setValue(null);
				row.getColumns().add(idColumn);
				byte[] idValue = translateOne(proxy);
				Field[] fieldsinClass = proxy.getClass().getDeclaredFields();
				for (Field singleField : fieldsinClass) {
					singleField.setAccessible(true);
					if(Modifier.isTransient(singleField.getModifiers()) ||
							Modifier.isStatic(singleField.getModifiers()) ||
							singleField.isAnnotationPresent(NoSqlTransient.class))
						continue;
					if (!singleField.isAnnotationPresent(NoSqlId.class)) {
						addColumn(proxy, singleField, idValue, row);
					}
				}
			} else {
				if (proxy != null) {
					Field[] fieldsinClass = proxy.getClass().getDeclaredFields();
					for (Field singleField : fieldsinClass) {
						singleField.setAccessible(true);
						if(Modifier.isTransient(singleField.getModifiers()) ||
								Modifier.isStatic(singleField.getModifiers()) ||
								singleField.isAnnotationPresent(NoSqlTransient.class))
							continue;
						addColumnWithoutId(proxy, singleField, row);
					}
				}
			}
		}
	}

	private void addColumn(PROXY proxy, Field singleField, byte[] idValue, RowToPersist row) {
		Object value = ReflectionUtil.fetchFieldValue(proxy, singleField);
		Column c = new Column();
		byte[] columnName = formTheColumnName(proxy, idValue, singleField);
		c.setName(columnName);
        if (value != null) {
            byte[] columnValue = null;
            NoSqlConverter customConv = singleField.getAnnotation(NoSqlConverter.class);
            if (customConv != null) {
                columnValue = getBytesValue(value, customConv);
            } else {
                columnValue = StandardConverters.convertToBytes(value);
            }
            c.setValue(columnValue);
        }
		row.getColumns().add(c);
	}

	private byte[] formTheColumnName(PROXY p, byte[] id, Field singleField) {
		byte[] prefix = StandardConverters.convertToBytes(columnName);
		byte[] singleFieldName = StandardConverters.convertToBytes(singleField.getName());
		byte[] columnName = new byte[ prefix.length + id.length + singleFieldName.length];
		for (int i = 0; i < columnName.length; i++) {
			if (i < prefix.length)
				columnName[i] = prefix[i];
			else if (i < (prefix.length + id.length))
				columnName[i] = id[i - prefix.length];
			else 
				columnName[i] = singleFieldName[i - prefix.length - id.length];
		}
		return columnName;
	}

	private byte[] formTheName(PROXY p) {
		byte[] pkData = translateOne(p);
		if (pkData == null)
			return null;
		else 
			return formTheNameImpl(columnName, pkData);
	}

	private Object createProxy(byte[] rowKey, Row row) {
		Object newproxy = null;
		try {
			newproxy = classMeta.getMetaClass().newInstance();
		} catch (InstantiationException e) {
			throw new UnsupportedOperationException("There is some problem in creating the proxy");
		} catch (IllegalAccessException e) {
			throw new UnsupportedOperationException("There is some problem in creating the proxy");
		}
		DboColumnMeta colMeta = this.getMetaDbo();
		if (classMeta.getIdField() != null && (rowKey!=null)) {
			// first fill the id
			DboColumnEmbedMeta embedMeta = (DboColumnEmbedMeta) colMeta;
            Object idValue = null;
            MetaField<PROXY> metaFieldId = classMeta.getIdField();
            NoSqlConverter customConvId = metaFieldId.getField().getAnnotation(NoSqlConverter.class);
            byte[] idBytes = null;
            if (customConvId != null) {
                idValue = getValue(rowKey, customConvId);
                idBytes = getBytesValue(idValue, customConvId);
            } else {
                idValue = embedMeta.getFkToColumnFamily().getIdColumnMeta().getStorageType().convertFromNoSql(rowKey);
                idBytes = StandardConverters.convertToBytes(idValue);
            }
            if (metaFieldId != null)
                ReflectionUtil.putFieldValue(newproxy, metaFieldId.getField(),idValue);

			// Now extract other columns
			byte[] prefix = StandardConverters.convertToBytes(getColumnName());
			byte[] embedColumn = new byte[prefix.length + idBytes.length];
		    System.arraycopy(prefix,0,embedColumn,0         ,prefix.length);
		    System.arraycopy(idBytes,0,embedColumn,prefix.length,idBytes.length);
			Collection<Column> columnsInRow = row.columnByPrefix(embedColumn);
			for (Column colInRow : columnsInRow) {
				byte[] fullNameCol = colInRow.getName();
				int colLen = fullNameCol.length - embedColumn.length;
				byte[] fk = new byte[colLen];
				for (int i = embedColumn.length; i < fullNameCol.length; i++) {
					fk[i - embedColumn.length] = fullNameCol[i];
				}

                Object colVal = colMeta.convertFromStorage2(fk);
                String colName = colMeta.convertTypeToString(colVal);

                Object columnValue = null;
                MetaField<PROXY> metaField = classMeta.getMetaFieldByCol(null, colName);
                if(metaField == null)
                	continue; //skip this field sice we don't know this column
                NoSqlConverter customConv = metaField.getField().getAnnotation(NoSqlConverter.class);
                if (customConv != null) {
                    columnValue = getValue(colInRow.getValue(), customConv);
                } else {
                    columnValue = embedMeta.getFkToColumnFamily().getColumnMeta(colName).getStorageType().convertFromNoSql(colInRow.getValue());
                }
                if (metaField != null)
					ReflectionUtil.putFieldValue(newproxy, metaField.getField(), columnValue);
			}
		} else {
			// No Id is present, only fill the columns
		    createProxy2(colMeta, newproxy, row);
		}
		return newproxy;
	}

    private void createProxy2(DboColumnMeta colMeta, Object newproxy, Row row) {
        byte[] colName = StandardConverters.convertToBytes(getColumnName());
        Collection<Column> columnsWORowKey = row.columnByPrefix(colName);
        for (Column col : columnsWORowKey) {
            byte[] fullName = col.getName();
            int embedColumnLen = fullName.length - colName.length;
            byte[] embedColumn = new byte[embedColumnLen];
            for (int i = colName.length; i < fullName.length; i++) {
                embedColumn[i - colName.length] = fullName[i];
            }
            Object colVal = colMeta.convertFromStorage2(embedColumn);
            String columnName = colMeta.convertTypeToString(colVal);

            DboColumnEmbedMeta embedMeta = (DboColumnEmbedMeta) colMeta;
            if (embedMeta.getFkToColumnFamily().getColumnMeta(columnName) == null)
                continue;
            Object columnValue = null;
            MetaField<PROXY> metaField = classMeta.getMetaFieldByCol(null, columnName);
            NoSqlConverter customConv = metaField.getField().getAnnotation(NoSqlConverter.class);
            if (customConv != null) {
                columnValue = getValue(col.getValue(), customConv);
            } else {
                columnValue = embedMeta.getFkToColumnFamily().getColumnMeta(columnName).getStorageType().convertFromNoSql(col.getValue());
            }
            if (classMeta.getMetaFieldByCol(null, columnName) != null)
                ReflectionUtil.putFieldValue(newproxy, classMeta.getMetaFieldByCol(null, columnName).getField(), columnValue);
        }
    }

    private byte[] formTheNameImpl(String colName, byte[] postFix) {
		byte[] prefix = StandardConverters.convertToBytes(colName);
		byte[] rowid = StandardConverters.convertToBytes("Id");
		byte[] name = new byte[prefix.length + rowid.length + postFix.length];
		for (int i = 0; i < name.length; i++) {
			if (i < prefix.length)
				name[i] = prefix[i];
			else if (i < (prefix.length + rowid.length))
				name[i] = rowid[i - prefix.length];
			else 
				name[i] = postFix[i - prefix.length - rowid.length];
		}
		return name;
	}

    private byte[] translateOne(PROXY proxy) {
        byte[] byteVal = classMeta.convertEntityToId(proxy);
        return byteVal;
    }

	private List<byte[]> parseColNamePostfix(String columnName, Row row) {
		String columnNameWithPrefix = columnName + "Id";
		byte[] namePrefix = StandardConverters.convertToBytes(columnNameWithPrefix);
		Collection<Column> columns = row.columnByPrefix(namePrefix);
		List<byte[]> entities = new ArrayList<byte[]>();
		
		for(Column col : columns) {
			byte[] rowkeyFullName = col.getName();
			int rkLen = rowkeyFullName.length-namePrefix.length;
			byte[] rk = new byte[rkLen];
			for(int i = namePrefix.length; i < rowkeyFullName.length; i++) {
				rk[i-namePrefix.length] =  rowkeyFullName[i];
			}
			entities.add(rk);
		}
		return entities;
	}

	private void addColumnWithoutId(PROXY proxy, Field singleField, RowToPersist row) {
		Object value = ReflectionUtil.fetchFieldValue(proxy, singleField);
		Column c = new Column();
		//byte[] columnName = formTheColumnName(proxy, idValue, singleField);

		byte[] prefix = StandardConverters.convertToBytes(columnName);
		byte[] singleFieldName = StandardConverters.convertToBytes(singleField.getName());
		byte[] columnName = new byte[ prefix.length + singleFieldName.length];
		for (int i = 0; i < columnName.length; i++) {
			if (i < prefix.length)
				columnName[i] = prefix[i];
			else
				columnName[i] = singleFieldName[i - prefix.length];
		}
		c.setName(columnName);
		if (value != null) {
            byte[] columnValue = null;
            NoSqlConverter customConv = singleField.getAnnotation(NoSqlConverter.class);
            if (customConv != null) {
                columnValue = getBytesValue(value, customConv);
            } else {
                columnValue = StandardConverters.convertToBytes(value);
            }
            c.setValue(columnValue);
		}
		row.getColumns().add(c);
	}

	@Override
	public void removingEntity(InfoForIndex<OWNER> info,
			List<IndexData> indexRemoves, byte[] rowKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] translateValue(Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object fetchField(Object entity) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String translateToString(Object fieldsValue) {
		throw new UnsupportedOperationException();
	}

	@Override
	public DboColumnMeta getMetaDbo() {
		return metaDbo;
	}

	@Override
	protected Object unwrapIfNeeded(Object value) {
		throw new UnsupportedOperationException();
	}

    private byte[] getBytesValue(Object value, NoSqlConverter customConv) {
        Class<? extends Converter> convClazz = customConv.converter();
        Converter converter = ReflectionUtil.create(convClazz);
        return converter.convertToNoSql(value);
    }

    private Object getValue(byte[] value, NoSqlConverter customConv) {
        Class<? extends Converter> convClazz = customConv.converter();
        Converter converter = ReflectionUtil.create(convClazz);
        return converter.convertFromNoSql(value);
    }
}
