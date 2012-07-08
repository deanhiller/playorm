package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import javassist.util.proxy.Proxy;

import com.alvazan.orm.api.base.Converter;
import com.alvazan.orm.api.base.spi.KeyGenerator;
import com.alvazan.orm.api.spi.db.Row;
import com.alvazan.orm.api.spi.layer2.NoSqlSession;
import com.alvazan.orm.impl.meta.query.MetaFieldDbo;

//NOTE: T is the entity type NOT the type of the id!!!
public class MetaIdField<OWNER> {

	private MetaFieldDbo metaDbo = new MetaFieldDbo();
	protected Field field;
	private Converter converter;
	
	private boolean useGenerator;
	private KeyGenerator generator;
	private Method method;
	private MetaClass<OWNER> metaClass;

	@Override
	public String toString() {
		return "MetaField [field='" + field.getDeclaringClass().getName()+"."+field.getName()+"(field type=" +field.getType()+ ")";
	}

	public Object translateFromRow(Row row, OWNER entity) {
		byte[] rowKey = row.getKey();
		Object entityId = converter.convertFromNoSql(rowKey);
		ReflectionUtil.putFieldValue(entity, field, entityId);
		return entityId;
	}
	
	public void translateToRow(OWNER entity, RowToPersist row) {
		Object idInEntity = ReflectionUtil.fetchFieldValue(entity, field);
		Object id = fetchFinalId(idInEntity, entity);
		byte[] byteVal = converter.convertToNoSql(id);
		row.setKey(byteVal);
	}

	private Object fetchFinalId(Object idInEntity, OWNER entity) {
		Object id = idInEntity;
		if(!useGenerator) {
			if(id == null)
				throw new IllegalArgumentException("Entity has @NoSqlEntity(usegenerator=false) but this entity has no id="+entity);
			return id;
		} else if(id != null)
			return id;
		
		Object newId = generator.generateNewKey(entity);
		ReflectionUtil.putFieldValue(entity, field, newId);
		return newId;
	}

	public void setup(Field field2, Method idMethod, boolean useGenerator, KeyGenerator gen, Converter converter, MetaClass<OWNER> metaClass) {
		this.field = field2;
		this.method = idMethod;
		this.field.setAccessible(true);
		this.method.setAccessible(true);
		this.useGenerator = useGenerator;
		this.generator = gen;
		this.converter = converter;
		this.metaClass = metaClass;
		metaDbo.setup(field.getName(), null, field.getType().getName());
	}

	public Converter getConverter() {
		return converter;
	}

	public Field getField() {
		return field;
	}

	public Method getIdMethod() {
		return method;
	}

	public OWNER convertIdToProxy(NoSqlSession session, Object entityId) {
		if(entityId == null)
			return null;
		OWNER proxy = createProxy(entityId, session);
		ReflectionUtil.putFieldValue(proxy, field, entityId);
		return proxy;
	}
	
	@SuppressWarnings("unchecked")
	private OWNER createProxy(Object entityId, NoSqlSession session) {
		Class<?> subclassProxyClass = metaClass.getProxyClass();
		Proxy inst = (Proxy) ReflectionUtil.create(subclassProxyClass);
		inst.setHandler(new NoSqlProxyImpl<OWNER>(session, metaClass, entityId));
		return (OWNER) inst;
	}

	public MetaFieldDbo getMetaDbo() {
		return metaDbo;
	}

	
}
