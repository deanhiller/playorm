package com.alvazan.orm.impl.meta;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import com.alvazan.orm.api.Converter;
import com.alvazan.orm.api.anno.Column;
import com.alvazan.orm.api.anno.Id;
import com.alvazan.orm.api.anno.ManyToOne;
import com.alvazan.orm.api.anno.NoConversion;
import com.alvazan.orm.api.anno.NoSqlEntity;
import com.alvazan.orm.api.anno.OneToOne;
import com.alvazan.orm.api.spi.KeyGenerator;
import com.google.inject.Provider;

public class InspectorField {

	@Inject
	private MetaInfo metaInfo;
	@Inject
	private Provider<MetaIdField> idMetaProvider;
	@Inject
	private Provider<MetaCommonField> metaProvider;
	@Inject
	private Provider<MetaProxyField> metaProxyProvider;
	
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
		
		String colName = field.getName();
		if(!"".equals(idAnno.columnName()))
			colName = idAnno.columnName();
		
		Class<?> type = field.getType();
		Converter converter = null;
		if(!NoConversion.class.isAssignableFrom(idAnno.customConverter()))
			converter = ReflectionUtil.create(idAnno.customConverter());
		
		try {
			converter = lookupConverter(type, converter);
			metaField.setup(field, colName, idAnno.usegenerator(), gen, converter);
			return metaField;			
		} catch(IllegalArgumentException e)	{
			throw new IllegalArgumentException("No converter found for field='"+field.getName()+"' in class="
					+field.getDeclaringClass()+".  You need to either add on of the @*ToOne annotations, @Embedded, " +
							"or add your own converter calling EntityMgrFactory.setup(Map<Class, Converter>) which " +
							"will then work for all fields of that type OR add @Column(customConverter=YourConverter.class)" +
							" or @Id(customConverter=YourConverter.class) " +
							" or finally if we missed a standard converter, we need to add it in file InspectorField.java" +
							" in the constructor and it is trivial code(and we can copy the existing pattern)");
		}		 
	}

	public MetaField processColumn(Field field) {
		Column col = field.getAnnotation(Column.class);
		MetaCommonField metaField = metaProvider.get();
		String colName = field.getName();
		if(col != null) {
			if(!"".equals(col.columnName()))
				colName = col.columnName();
		}
		
		Class<?> type = field.getType();
		Converter converter = null;
		if(col != null && !NoConversion.class.isAssignableFrom(col.customConverter()))
			converter = ReflectionUtil.create(col.customConverter());
		
		try {
			converter = lookupConverter(type, converter);
			metaField.setup(field, colName, converter);
			return metaField;			
		} catch(IllegalArgumentException e)	{
			throw new IllegalArgumentException("No converter found for field='"+field.getName()+"' in class="
					+field.getDeclaringClass()+".  You need to either add on of the @*ToOne annotations, @Embedded, " +
							"or add your own converter calling EntityMgrFactory.setup(Map<Class, Converter>) which " +
							"will then work for all fields of that type OR add @Column(customConverter=YourConverter.class)" +
							" or finally if we missed a standard converter, we need to add it in file InspectorField.java" +
							" in the constructor and it is trivial code(and we can copy the existing pattern)");
		}
	}
	
	private Converter lookupConverter(Class<?> type, Converter custom) {
		if(custom != null) {
			return custom;
		} else if(customConverters.get(type) != null) {
			return customConverters.get(type);
		} else if(stdConverters.get(type) != null){
			return stdConverters.get(type);
		}
		throw new IllegalArgumentException("bug, caller should catch this and log info about field or id converter, etc. etc");
	}

	public MetaField processOneToMany(Field field) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	public MetaField processEmbeddable(Field field) {
		throw new UnsupportedOperationException("not implemented yet");
	}

	@SuppressWarnings("rawtypes")
	public void setCustomConverters(Map<Class, Converter> converters) {
		if(converters == null)
			return; //nothing to do
		
		this.customConverters = converters;
	}

	public MetaField processManyToOne(Field field) {
		ManyToOne annotation = field.getAnnotation(ManyToOne.class);
		String colName = field.getName();
		if(annotation != null) {
			if(!"".equals(annotation.columnName()))
				colName = annotation.columnName();
		}
		
		return processToOne(field, colName);
	}

	public MetaField processOneToOne(Field field) {
		OneToOne annotation = field.getAnnotation(OneToOne.class);
		String colName = field.getName();
		if(annotation != null) {
			if(!"".equals(annotation.columnName()))
				colName = annotation.columnName();
		}
		
		return processToOne(field, colName);
	}
	
	public MetaField processToOne(Field field, String colName) {
		//at this point we only need to verify that 
		//the class referred has the @NoSqlEntity tag so it is picked up by scanner
		if(!field.getType().isAnnotationPresent(NoSqlEntity.class))
			throw new RuntimeException("type="+field.getType()+" needs the NoSqlEntity annotation" +
					" since field has *ToOne annotation.  field="+field.getDeclaringClass().getName()+"."+field.getName());
		
		MetaProxyField metaField = metaProxyProvider.get();
		MetaClass<?> classMeta = metaInfo.findOrCreate(field.getType());
		metaField.setup(field, colName, classMeta);
		return metaField;
	}
}
