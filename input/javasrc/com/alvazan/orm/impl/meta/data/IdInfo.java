package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Method;

import com.alvazan.orm.api.base.spi.KeyGenerator;
import com.alvazan.orm.api.spi3.db.conv.Converter;

public class IdInfo {
	private Method idMethod;
	private boolean useGenerator;
	private KeyGenerator gen;
	private Converter converter;
	private MetaClass metaClass;
	public Method getIdMethod() {
		return idMethod;
	}
	public void setIdMethod(Method idMethod) {
		this.idMethod = idMethod;
	}
	public boolean isUseGenerator() {
		return useGenerator;
	}
	public void setUseGenerator(boolean useGenerator) {
		this.useGenerator = useGenerator;
	}
	public KeyGenerator getGen() {
		return gen;
	}
	public void setGen(KeyGenerator gen) {
		this.gen = gen;
	}
	public Converter getConverter() {
		return converter;
	}
	public void setConverter(Converter converter) {
		this.converter = converter;
	}
	public MetaClass getMetaClass() {
		return metaClass;
	}
	public void setMetaClass(MetaClass metaClass) {
		this.metaClass = metaClass;
	}
	
}
