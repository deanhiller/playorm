package com.alvazan.orm.impl.meta.data;

import java.lang.reflect.Method;

import com.alvazan.orm.api.base.spi.KeyGenerator;
import com.alvazan.orm.api.z8spi.conv.Converter;

@SuppressWarnings("rawtypes")
public class IdInfo {
	private Method idMethod;
	private boolean useGenerator;
	private KeyGenerator gen;
	private Converter converter;
	private MetaAbstractClass metaClass;
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
	public MetaAbstractClass getMetaClass() {
		return metaClass;
	}
	public void setMetaClass(MetaAbstractClass metaClass) {
		this.metaClass = metaClass;
	}
	
}
