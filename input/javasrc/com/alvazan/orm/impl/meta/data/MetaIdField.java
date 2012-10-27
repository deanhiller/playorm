package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import javassist.util.proxy.Proxy;

import com.alvazan.orm.api.base.spi.KeyGenerator;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.conv.Converter;
import com.alvazan.orm.api.z8spi.conv.StorageTypeEnum;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.IndexData;
import com.alvazan.orm.api.z8spi.meta.InfoForIndex;
import com.alvazan.orm.api.z8spi.meta.ReflectionUtil;
import com.alvazan.orm.api.z8spi.meta.RowToPersist;
import com.alvazan.orm.impl.meta.data.collections.CacheLoadCallback;
import com.eaio.uuid.UUID;

//NOTE: T is the entity type NOT the type of the id!!!
public class MetaIdField<OWNER> extends MetaAbstractField<OWNER> {

	private Converter converter;
	
	private boolean useGenerator;
	private KeyGenerator generator;
	private Method method;
	private MetaAbstractClass<OWNER> metaClass;
	private DboColumnIdMeta metaDbo = new DboColumnIdMeta();

	public DboColumnMeta getMetaDbo() {
		return metaDbo;
	}
	
	@Override
	public String toString() {
		return "MetaField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType()+ ")";
	}

	public void translateFromColumn(Row row, OWNER entity, NoSqlSession session) {
		byte[] virtKey = row.getKey();
		byte[] nonVirtKey = metaDbo.unformVirtRowKey(virtKey);
		Object entityId = converter.convertFromNoSql(nonVirtKey);
		ReflectionUtil.putFieldValue(entity, field, entityId);
	}
	
	@Override
	public void translateToColumn(InfoForIndex<OWNER> info) {
		OWNER entity = info.getEntity();
		RowToPersist row = info.getRow();
		
		Object id = fillInAndFetchId(entity);
		byte[] byteVal = converter.convertToNoSql(id);
		byte[] virtualKey = metaDbo.formVirtRowKey(byteVal);
		row.setKeys(byteVal, virtualKey);
		
		StorageTypeEnum storageType = metaDbo.getStorageType();
		addIndexInfo(info, id, byteVal, storageType);
		//NOTICE: there is no call to remove because if an id is changed, it is a new entity and we only need to add index not remove since
		//the old entity was not deleted during this save....we remove from index on remove of old entity only
	}

	@Override
	public Object fetchField(Object entity) {
		throw new UnsupportedOperationException("only used for partitioning and id can't partition.  easy to implement if anyone else starts using this though, but for now unsupported");
	}

	@Override
	public String translateToString(Object fieldsValue) {
		throw new UnsupportedOperationException("only used for partitioning and id can't partition.  easy to implement if anyone else starts using this though, but for now unsupported");
	}
	
	@Override
	public void removingEntity(InfoForIndex<OWNER> info, List<IndexData> indexRemoves, byte[] pk) {
		removingThisEntity(info, indexRemoves, pk);
	}
	
	public Object fillInAndFetchId(OWNER entity) {
		Object idInEntity = ReflectionUtil.fetchFieldValue(entity, field);
		Object id = idInEntity;
		if(!useGenerator) {
			if(id == null)
				throw new IllegalArgumentException("Entity has @NoSqlEntity(usegenerator=false) but this entity has no id="+entity);
			return id;
		} else if(id != null) {
			//to make it easier on users, we now check and read in from database if they have not already
			
			//OKAY, we definitely do NOT want them setting their own id when autogeneration is used as IF you set the id on TWO entities
			//to the same id and add both of them, you end up with a corrupt index in that you have duplicates of two values pointing to
			//the same exact primary key.
//			if(!(entity instanceof NoSqlProxy))
//				throw new IllegalArgumentException("Uhm, uh, you have useGenerator=true(the default) on @NoSqlId annotation and the entity you " +
//						"passed in was NOT read from the database!!!!  You supplied " +
//						"your own id which could exist in the database...this will cause LARGE issues with indexing so we don't allow it, please don't set the id OR you" +
//						" are using a primitive for your key which is not a good idea either if you are going to use a generator(use Integer or String or Long instead)");
			return id;
		}
		Object newId;
		if (field.getType().equals(UUID.class))
			newId = new UUID();
		else 
			newId = generator.generateNewKey(entity);
		ReflectionUtil.putFieldValue(entity, field, newId);
		return newId;
	}

	@SuppressWarnings("unchecked")
	public void setup(DboTableMeta tableMeta, IdInfo info, Field field, String columnName,	boolean isIndexed) {
		this.method = info.getIdMethod();
		this.method.setAccessible(true);
		this.useGenerator = info.isUseGenerator();
		this.generator = info.getGen();
		this.converter = info.getConverter();	
		this.metaClass = info.getMetaClass();
		metaDbo.setup(tableMeta, columnName, field.getType(), isIndexed);
		super.setup(field, columnName);
	}

	public Converter getConverter() {
		return converter;
	}

	public Method getIdMethod() {
		return method;
	}

	public OWNER convertIdToProxy(NoSqlSession session, Object entityId, CacheLoadCallback cacheLoadCallback) {
		if(entityId == null)
			return null;
		OWNER proxy = createProxy(entityId, session, cacheLoadCallback);
		ReflectionUtil.putFieldValue(proxy, field, entityId);
		return proxy;
	}
	
	@SuppressWarnings("unchecked")
	private OWNER createProxy(Object entityId, NoSqlSession session, CacheLoadCallback cacheLoadCallback) {
		Class<?> subclassProxyClass = metaClass.getProxyClass();
		Proxy inst = (Proxy) ReflectionUtil.create(subclassProxyClass);
		inst.setHandler(new NoSqlProxyImpl<OWNER>(session, metaClass, entityId, cacheLoadCallback));
		return (OWNER) inst;
	}

	@Override
	protected Object unwrapIfNeeded(Object value) {
		return value; // no need to unwrap keys
	}

	@Override
	public byte[] translateValue(Object value) {
		return converter.convertToNoSql(value);
	}

	public DboColumnIdMeta getMetaIdDbo() {
		return metaDbo;
	}

	public Object translateFromBytes(byte[] val) {
		return converter.convertFromNoSql(val);
	}

	public boolean isAutoGen() {
		return useGenerator;
	}

	public byte[] convertIdToNonVirtKey(Object pk) {
		return converter.convertToNoSql(pk);
	}

	public byte[] formVirtRowKey(byte[] rowKey) {
		byte[] virtKey = metaDbo.formVirtRowKey(rowKey);
		return virtKey;
	}

	public byte[] unformVirtRowKey(byte[] virtualKey) {
		return metaDbo.unformVirtRowKey(virtualKey);
	}

}
