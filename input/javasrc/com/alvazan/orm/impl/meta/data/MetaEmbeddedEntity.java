package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alvazan.orm.api.base.ToOneProvider;
import com.alvazan.orm.api.exc.ChildWithNoPkException;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.meta.DboColumnEmbedMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.RowToPersist;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.meta.InfoForIndex;
import com.alvazan.orm.api.z8spi.meta.ReflectionUtil;
import com.alvazan.orm.impl.meta.data.collections.ListProxyFetchAll;
import com.alvazan.orm.impl.meta.data.collections.MapProxyFetchAll;
import com.alvazan.orm.impl.meta.data.collections.OurAbstractCollection;
import com.alvazan.orm.impl.meta.data.collections.SetProxyFetchAll;
import com.alvazan.orm.impl.meta.data.collections.ToOneProviderProxy;

public class MetaEmbeddedEntity<OWNER, PROXY, T> extends MetaAbstractField<OWNER> {

	private DboColumnEmbedMeta metaDbo = new DboColumnEmbedMeta();
	private MetaAbstractClass<PROXY> classMeta;
	private Field fieldForKey;

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
		Object proxy;
		String columnName = getColumnName();
		byte[] colBytes = StandardConverters.convertToBytes(columnName);
		Column column = row.getColumn(colBytes);
		if (column == null) {
			column = new Column();
		}
		if (field.getType().equals(ToOneProvider.class)) {
			proxy = translateFromToComposite(row, session);
			if (proxy == null)
				proxy = translateFromToProxy(row, column.getValue(), session);
		} else {
			proxy = convertIdToProxyComposite(row, session);
			if (proxy == null)
				proxy = convertIdToProxy(row, column.getValue(), session);
		}
		return proxy;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Object translateFromToProxy(Row row, byte[] value,
			NoSqlSession session) {
		ToOneProvider<PROXY> toOne = new ToOneProviderProxy(classMeta, value,
				session);
		return toOne;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Object translateFromToComposite(Row row, NoSqlSession session) {
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

	public PROXY convertIdToProxy(Row row, byte[] nonVirtFk,
			NoSqlSession session) {
		Tuple<PROXY> tuple = classMeta.convertIdToProxy(row, session,
				nonVirtFk, null);
		return tuple.getProxy();
	}

	private Object convertIdToProxyComposite(Row row, NoSqlSession session) {
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
			Tuple<PROXY> tuple = classMeta.convertIdToProxy(row, session, fk,
					null);
			return tuple.getProxy();
		} else
			return null;
	}

	private Object translateFromColumnSet(Row row, OWNER entity,
			NoSqlSession session) {
		List<byte[]> keys = MetaToManyField.parseColNamePostfix(columnName, row);
		Set<PROXY> retVal = new SetProxyFetchAll<PROXY>(entity, session,
				classMeta, keys, field);
		return retVal;
	}

	@SuppressWarnings({ "rawtypes" })
	private Map translateFromColumnMap(Row row, OWNER entity,
			NoSqlSession session) {
		List<byte[]> keys = MetaToManyField.parseColNamePostfix(columnName, row);
		MapProxyFetchAll proxy = MapProxyFetchAll.create(entity, session,
				classMeta, keys, fieldForKey, field);
		return proxy;
	}

	private List<PROXY> translateFromColumnList(Row row, OWNER entity,
			NoSqlSession session) {
		List<byte[]> keys = MetaToManyField.parseColNamePostfix(columnName, row);
		List<PROXY> retVal = new ListProxyFetchAll<PROXY>(entity, session,
				classMeta, keys, field);
		return retVal;
	}

	@Override
	public void translateToColumn(InfoForIndex<OWNER> info) {
		OWNER entity = info.getEntity();
		RowToPersist row = info.getRow();
		if (field.getType().equals(Map.class))
			translateToColumnMap(entity, row);
		else
			translateToColumnList(entity, row);
	}

	@SuppressWarnings("unchecked")
	private void translateToColumnList(OWNER entity, RowToPersist row) {
		Collection<PROXY> values = (Collection<PROXY>) ReflectionUtil
				.fetchFieldValue(entity, field);
		Collection<PROXY> toBeAdded = values; // all values in the list get
												// added if not an
												// OurAbstractCollection
		Collection<PROXY> toBeRemoved = new ArrayList<PROXY>();
		if (values instanceof OurAbstractCollection) {
			OurAbstractCollection<PROXY> coll = (OurAbstractCollection<PROXY>) values;
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

		// now process all the existing columns (we can add same entity as many
		// times as we like and it does not
		// get duplicated)
		if (toBeAdded != null) {
			for (PROXY proxy : toBeAdded) {
				byte[] name = formTheName(proxy);
				Column c = new Column();
				c.setName(name);

				row.getColumns().add(c);
			}
		}
	}

	private byte[] formTheName(PROXY p) {
		byte[] pkData = translateOne(p);
		return formTheNameImpl(columnName, pkData);
	}

	static byte[] formTheNameImpl(String colName, byte[] postFix) {
		byte[] prefix = StandardConverters.convertToBytes(colName);
		byte[] name = new byte[prefix.length + postFix.length];
		for (int i = 0; i < name.length; i++) {
			if (i < prefix.length)
				name[i] = prefix[i];
			else
				name[i] = postFix[i - prefix.length];
		}
		return name;
	}

	private byte[] translateOne(PROXY proxy) {
		byte[] byteVal = classMeta.convertEntityToId(proxy);
		if(byteVal == null) {
			String owner = "'"+field.getDeclaringClass().getSimpleName()+"'";
			String child = "'"+classMeta.getMetaClass().getSimpleName()+"'";
			String fieldName = "'"+field.getType().getSimpleName()+" "+field.getName()+"'";
			throw new ChildWithNoPkException("The entity you are saving of type="+owner+" has a field="+fieldName
					+" which has an entity in the collection that does not yet have a primary key so you cannot save it. \n" +
					"The offending object is="+proxy+"   To correct this\n" +
					"problem, you can either\n"
					+"1. SAVE the "+child+" BEFORE you save the "+owner+" OR\n"
					+"2. Call entityManager.fillInWithKey(Object entity), then SAVE your "+owner+"', then save your "+child+" NOTE that this" +
							"\nmethod #2 is used for when you have a bi-directional relationship where each is a child of the other");
		}
		return byteVal;
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

}
