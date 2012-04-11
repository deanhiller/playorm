package com.alvazan.orm.impl.meta;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import com.alvazan.orm.api.Converter;
import com.alvazan.orm.api.anno.Column;
import com.alvazan.orm.api.anno.Id;
import com.alvazan.orm.api.anno.NoConversion;
import com.alvazan.orm.api.spi.KeyGenerator;
import com.google.inject.Provider;

public class InspectorField {

	@Inject
	private Provider<MetaIdField> idMetaProvider;
	@Inject
	private Provider<MetaField> metaProvider;
	@SuppressWarnings("rawtypes")
	private Map<Class, Converter> customConverters = new HashMap<Class, Converter>();
	@SuppressWarnings("rawtypes")
	private Map<Class, Converter> stdConverters = new HashMap<Class, Converter>();
	
	public InspectorField() {
		stdConverters.put(short.class, new Converters.ShortConverter());
		stdConverters.put(Short.class, new Converters.ShortConverter());
		stdConverters.put(int.class, new Converters.IntConverter());
		stdConverters.put(Integer.class, new Converters.IntConverter());
		stdConverters.put(long.class, new Converters.LongConverter());
		stdConverters.put(Long.class, new Converters.LongConverter());
		stdConverters.put(float.class, new Converters.FloatConverter());
		stdConverters.put(Float.class, new Converters.FloatConverter());
		stdConverters.put(double.class, new Converters.DoubleConverter());
		stdConverters.put(Double.class, new Converters.DoubleConverter());
		stdConverters.put(byte.class, new Converters.ByteConverter());
		stdConverters.put(Byte.class, new Converters.ByteConverter());
		stdConverters.put(String.class, new Converters.StringConverter());
	}
	
	public MetaIdField processId(Field field) {
		if(!String.class.isAssignableFrom(field.getType()))
			throw new IllegalArgumentException("The id is not of type String and has to be.  field="+field+" in class="+field.getDeclaringClass());
		
		Id idAnno = field.getAnnotation(Id.class);
		MetaIdField metaField = idMetaProvider.get();
		KeyGenerator gen = null;
		if(idAnno.usegenerator()) {
			Class<? extends KeyGenerator> generation = idAnno.generation();
			gen = ReflectionUtil.create(generation);
		}
		
		metaField.setup(field, idAnno.usegenerator(), gen);
		
		return metaField;
	}

	public MetaField processColumn(Field field) {
		Column col = field.getAnnotation(Column.class);
		MetaField metaField = metaProvider.get();
		String colName = field.getName();
		if(col != null) {
			if(!"".equals(col.columnName()))
				colName = col.columnName();
		}
		
		Converter converter = null;
		if(col != null && !NoConversion.class.isAssignableFrom(col.customConverter())) {
			converter = ReflectionUtil.create(col.customConverter()); 
		} else if(customConverters.get(field.getType()) != null) {
			converter = customConverters.get(field.getType());
		} else if(stdConverters.get(field.getType()) != null){
			converter = stdConverters.get(field.getType());
		} else {
			throw new IllegalArgumentException("No converter found for field='"+field.getName()+"' in class="
					+field.getDeclaringClass()+".  You need to either add on of the @*ToOne annotations, @Embedded, " +
							"or add your own converter calling EntityMgrFactory.setup(Map<Class, Converter>) which " +
							"will then work for all fields of that type OR add @Column(customConverter=YourConverter.class)" +
							" or finally if we missed a standard converter, we need to add it in file InspectorField.java" +
							" in the constructor and it is trivial code(and we can copy the existing pattern)");
		}
		
		metaField.setup(field, colName, converter);
		return metaField;
	}
	
	public MetaField processToOne(Field field) {
		// TODO Auto-generated method stub
		return null;
	}

	public MetaField processOneToMany(Field field) {
		// TODO Auto-generated method stub
		return null;
	}

	public MetaField processEmbeddable(Field field) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("rawtypes")
	public void setCustomConverters(Map<Class, Converter> converters) {
		if(converters == null)
			return; //nothing to do
		
		this.customConverters = converters;
	}

}
