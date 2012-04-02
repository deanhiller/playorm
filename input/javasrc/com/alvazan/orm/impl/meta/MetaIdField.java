package com.alvazan.orm.impl.meta;

import java.lang.reflect.Field;

import com.alvazan.orm.api.spi.KeyGenerator;

public class MetaIdField {

	private Field field;
	private boolean useGenerator;
	private KeyGenerator generator;
	
	//This only exists here to be symetrical with fillInOrCheckId which is called
	//during persist.  This method is called during reads!!!
	//In the future, if we have more id translation, this class could explode with code
	public void fillInId(Object entity, String id) {
		ReflectionUtil.putFieldValue(entity, field, id);
	}
	
	public String fillInOrCheckForId(Object entity) {
		Object id = ReflectionUtil.fetchFieldValue(entity, field);
		if(!useGenerator) {
			if(id == null)
				throw new IllegalArgumentException("Entity has @NoSqlEntity(usegenerator=false) but this entity has no id="+entity);
			return (String)id;
		} else if(id != null)
			return (String)id;
		
		String newId = generator.generateNewKey(entity);
		ReflectionUtil.putFieldValue(entity, field, newId);
		return newId;
	}

	void setField(Field field2) {
		this.field = field2;
		this.field.setAccessible(true);
	}

	public void setup(Field field2, boolean useGenerator, KeyGenerator gen) {
		this.field = field2;
		this.field.setAccessible(true);
		this.useGenerator = useGenerator;
		this.generator = gen;
	}
}
