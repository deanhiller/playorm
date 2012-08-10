package com.alvazan.orm.impl.meta.data;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alvazan.orm.api.base.exc.ChildWithNoPkException;
import com.alvazan.orm.api.spi2.ColumnTypeEnum;
import com.alvazan.orm.api.spi2.DboTableMeta;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.db.Row;
import com.alvazan.orm.impl.meta.data.collections.ListProxyFetchAll;
import com.alvazan.orm.impl.meta.data.collections.MapProxyFetchAll;
import com.alvazan.orm.impl.meta.data.collections.OurAbstractCollection;
import com.alvazan.orm.impl.meta.data.collections.SetProxyFetchAll;

public final class MetaListField<OWNER, PROXY> extends MetaAbstractField<OWNER> {

	private MetaAbstractClass<PROXY> classMeta;
	private Field fieldForKey;
	
	@Override
	public void translateFromColumn(Row row, OWNER entity,
			NoSqlSession session) {
		Object proxy;
		if(field.getType().equals(Map.class))
			proxy = translateFromColumnMap(row, entity, session);
		else if(field.getType().equals(Collection.class)
				|| field.getType().equals(List.class))
			proxy = translateFromColumnList(row, entity, session);
		else if(field.getType().equals(Set.class))
			proxy = translateFromColumnSet(row, entity, session);
		else
			throw new RuntimeException("bug, we do not support type="+field.getType());
			
		ReflectionUtil.putFieldValue(entity, field, proxy);
	}

	private Object translateFromColumnSet(Row row, OWNER entity,
			NoSqlSession session) {
		List<byte[]> keys = parseOutKeyList(row);
		Set<PROXY> retVal = new SetProxyFetchAll<PROXY>(entity, session, classMeta, keys);
		return retVal;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Map translateFromColumnMap(Row row,
			OWNER entity, NoSqlSession session) {
		List<byte[]> keys = parseOutKeyList(row);
		MapProxyFetchAll proxy = MapProxyFetchAll.create(entity, session, classMeta, keys, fieldForKey);
		//MapProxyFetchAll proxy = new MapProxyFetchAll(entity, session, classMeta, keys, fieldForKey);
		return proxy;
	}
	
	private List<PROXY> translateFromColumnList(Row row,
			OWNER entity, NoSqlSession session) {
		List<byte[]> keys = parseOutKeyList(row);
		List<PROXY> retVal = new ListProxyFetchAll<PROXY>(entity, session, classMeta, keys);
		return retVal;
	}

	private List<byte[]> parseOutKeyList(Row row) {
		String columnName = getColumnName();
		byte[] bytes = columnName.getBytes();
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
	public void translateToColumn(InfoForIndex<OWNER> info) {
		OWNER entity = info.getEntity();
		RowToPersist row = info.getRow();
		if(field.getType().equals(Map.class))
			translateToColumnMap(entity, row);
		else
			translateToColumnList(entity, row);
	}

	@SuppressWarnings("unchecked")
	private void translateToColumnList(OWNER entity, RowToPersist row) {
		Collection<PROXY> values = (Collection<PROXY>) ReflectionUtil.fetchFieldValue(entity, field);
		Collection<PROXY> toBeAdded = values; //all values in the list get added if not an OurAbstractCollection
		Collection<PROXY> toBeRemoved = new ArrayList<PROXY>();
		if(values instanceof OurAbstractCollection) {
			OurAbstractCollection<PROXY> coll = (OurAbstractCollection<PROXY>)values;
			toBeRemoved = coll.getToBeRemoved();
			toBeAdded = coll.getToBeAdded();
		}
		
		translateToColumnImpl(toBeAdded, row, toBeRemoved);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void translateToColumnMap(OWNER entity, RowToPersist row) {
		Map mapOfProxies = (Map) ReflectionUtil.fetchFieldValue(entity, field);
		Collection collection = mapOfProxies.values();
		Collection<PROXY> toBeAdded = collection;
		Collection<PROXY> toBeRemoved = new ArrayList<PROXY>();
		if(mapOfProxies instanceof MapProxyFetchAll) {
			MapProxyFetchAll mapProxy = (MapProxyFetchAll) mapOfProxies;
			toBeRemoved = mapProxy.getToBeRemoved();
			toBeAdded = mapProxy.getToBeAdded();
		}
		
		translateToColumnImpl(toBeAdded, row, toBeRemoved);
	}

	private void translateToColumnImpl(Collection<PROXY> toBeAdded, RowToPersist row, Collection<PROXY> toBeRemoved) {
		//removes first
		for(PROXY p : toBeRemoved) {
			byte[] name = formTheName(p);
			row.addEntityToRemove(name);
		}
		
		//now process all the existing columns (we can add same entity as many times as we like and it does not
		//get duplicated)
		for(PROXY proxy : toBeAdded) {
			byte[] name = formTheName(proxy);
			Column c = new Column();
			c.setName(name);
			
			row.getColumns().add(c);
		}
	}
	
	private byte[] formTheName(PROXY p) {
		try {
			return formTheNameImpl(p);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
	private byte[] formTheNameImpl(PROXY p) throws UnsupportedEncodingException {
		byte[] pkData = translateOne(p);
		
		byte[] prefix = columnName.getBytes("UTF8");
		byte[] name = new byte[prefix.length + pkData.length];
		for(int i = 0; i < name.length; i++) {
			if(i < prefix.length)
				name[i] = prefix[i];
			else
				name[i] = pkData[i-prefix.length];
		}
		return name;
	}

	private byte[] translateOne(PROXY proxy) {
		//Value is the Account.java or a Proxy of Account.java field and what we need to save in 
		//the database is the ID inside this Account.java object!!!!
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

	public void setup(Field field, String colName, MetaAbstractClass<PROXY> classMeta, Field fieldForKey) {
		DboTableMeta fkToTable = classMeta.getMetaDbo();
		super.setup(field, colName, fkToTable, null, ColumnTypeEnum.LIST_OF_FK, null);
		this.classMeta = classMeta;
		this.fieldForKey = fieldForKey;
	}

	@Override
	public String toString() {
		return "MetaListField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType().getName()
				+ "<"+classMeta.getMetaClass().getName()+">), columnName=" + columnName + "]";
	}

	@Override
	public byte[] translateValue(Object value) {
		throw new UnsupportedOperationException("Bug, this operation shold never be called for lists");
	}

	@Override
	protected Object unwrapIfNeeded(Object value) {
		throw new UnsupportedOperationException("Bug, this should never be called");
	}

	@Override
	public void removingEntity(InfoForIndex<OWNER> info,
			List<IndexData> indexRemoves, byte[] rowKey) {
		throw new UnsupportedOperationException("Bug, this should never be called");		
	}
}
