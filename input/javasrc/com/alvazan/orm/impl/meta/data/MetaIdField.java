package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import javassist.util.proxy.Proxy;

import com.alvazan.orm.api.base.spi.KeyGenerator;
import com.alvazan.orm.api.spi2.ColumnTypeEnum;
import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi2.StorageTypeEnum;
import com.alvazan.orm.api.spi3.db.Row;
import com.alvazan.orm.api.spi3.db.conv.Converter;
import com.alvazan.orm.impl.meta.data.collections.CacheLoadCallback;

//NOTE: T is the entity type NOT the type of the id!!!
public class MetaIdField<OWNER> extends MetaAbstractField<OWNER> {

	private Converter converter;
	
	private boolean useGenerator;
	private KeyGenerator generator;
	private Method method;
	private MetaAbstractClass<OWNER> metaClass;

	@Override
	public String toString() {
		return "MetaField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType()+ ")";
	}

	public void translateFromColumn(Row row, OWNER entity, NoSqlSession session) {
		byte[] rowKey = row.getKey();
		Object entityId = converter.convertFromNoSql(rowKey);
		ReflectionUtil.putFieldValue(entity, field, entityId);
	}
	
	@Override
	public void translateToColumn(InfoForIndex<OWNER> info) {
		OWNER entity = info.getEntity();
		RowToPersist row = info.getRow();
		
		Object id = fillInAndFetchId(entity);
		byte[] byteVal = converter.convertToNoSql(id);
		row.setKey(byteVal);
		
		StorageTypeEnum storageType = getMetaDbo().getStorageType();
		addIndexInfo(info, id, byteVal, storageType);
		//NOTICE: there is no call to remove because if an id is changed, it is a new entity and we only need to add index not remove since
		//the old entity was not deleted during this save....we remove from index on remove of old entity only
	}

	@Override
	public void removingEntity(InfoForIndex<OWNER> info, List<IndexData> indexRemoves, byte[] pk) {
		StorageTypeEnum storageType = getMetaDbo().getStorageType();
		removingThisEntity(info, indexRemoves, pk, storageType);
	}
	
	public Object fillInAndFetchId(OWNER entity) {
		Object idInEntity = ReflectionUtil.fetchFieldValue(entity, field);
		Object id = idInEntity;
		if(!useGenerator) {
			if(id == null)
				throw new IllegalArgumentException("Entity has @NoSqlEntity(usegenerator=false) but this entity has no id="+entity);
			return id;
		} else if(id != null) {
			//kind of not a good idea if they new an entity and save it twice, this will then fail...should we enable this??? not sure.
//			if(!(entity instanceof NoSqlProxy))
//				throw new IllegalArgumentException("Uhm, uh, you have useGenerator=true(the default) on @Id annotation yet you supplied " +
//						"your own id...this will cause issues, please don't set the id OR you are using a primitive for your key which is not a good idea either if you are going to use a generator");
			return id;
		}
		
		Object newId = generator.generateNewKey(entity);
		ReflectionUtil.putFieldValue(entity, field, newId);
		return newId;
	}

	@SuppressWarnings("unchecked")
	public void setup(IdInfo info, Field field, String columnName,	String indexPrefix) {
		this.method = info.getIdMethod();
		this.method.setAccessible(true);
		this.useGenerator = info.isUseGenerator();
		this.generator = info.getGen();
		this.converter = info.getConverter();	
		this.metaClass = info.getMetaClass();
		super.setup(field, columnName, null, field.getType(), ColumnTypeEnum.ID, indexPrefix);
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

	
}
