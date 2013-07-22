package com.alvazan.orm.impl.meta.scan;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.CursorToMany;
import com.alvazan.orm.api.base.ToOneProvider;
import com.alvazan.orm.api.base.anno.NoSqlColumn;
import com.alvazan.orm.api.base.anno.NoSqlConverter;
import com.alvazan.orm.api.base.anno.NoSqlDiscriminatorColumn;
import com.alvazan.orm.api.base.anno.NoSqlEmbeddable;
import com.alvazan.orm.api.base.anno.NoSqlEmbedded;
import com.alvazan.orm.api.base.anno.NoSqlEntity;
import com.alvazan.orm.api.base.anno.NoSqlId;
import com.alvazan.orm.api.base.anno.NoSqlIndexed;
import com.alvazan.orm.api.base.anno.NoSqlInheritance;
import com.alvazan.orm.api.base.anno.NoSqlManyToMany;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlOneToMany;
import com.alvazan.orm.api.base.anno.NoSqlOneToOne;
import com.alvazan.orm.api.base.anno.NoSqlPartitionByThisField;
import com.alvazan.orm.api.base.spi.KeyGenerator;
import com.alvazan.orm.api.z5api.NoConversion;
import com.alvazan.orm.api.z8spi.conv.Converter;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.ReflectionUtil;
import com.alvazan.orm.impl.meta.data.IdInfo;
import com.alvazan.orm.impl.meta.data.MetaAbstractClass;
import com.alvazan.orm.impl.meta.data.MetaClassInheritance;
import com.alvazan.orm.impl.meta.data.MetaClassSingle;
import com.alvazan.orm.impl.meta.data.MetaCommonField;
import com.alvazan.orm.impl.meta.data.MetaCursorField;
import com.alvazan.orm.impl.meta.data.MetaEmbeddedEntity;
import com.alvazan.orm.impl.meta.data.MetaEmbeddedSimple;
import com.alvazan.orm.impl.meta.data.MetaField;
import com.alvazan.orm.impl.meta.data.MetaIdField;
import com.alvazan.orm.impl.meta.data.MetaInfo;
import com.alvazan.orm.impl.meta.data.MetaProxyField;
import com.alvazan.orm.impl.meta.data.MetaToManyField;

@SuppressWarnings("rawtypes")
public class ScannerForField {
	private static final Logger log = LoggerFactory.getLogger(ScannerForField.class);
	
	@Inject
	private MetaInfo metaInfo;
	@Inject
	private Provider<MetaIdField> idMetaProvider;
	@Inject
	private Provider<MetaCommonField> metaProvider;
	@Inject
	private Provider<MetaToManyField> metaListProvider;
	@Inject
	private Provider<MetaEmbeddedEntity> metaEmbeddedProvider;
	@Inject
	private Provider<MetaEmbeddedSimple> metaEmbeddedSimpleProvider;
	@Inject
	private Provider<MetaCursorField> metaCursorProvider;
	@Inject
	private Provider<MetaProxyField> metaProxyProvider;
	
	private Map<Class, Converter> customConverters = new HashMap<Class, Converter>();

	
	public ScannerForField() {

	}
	
	@SuppressWarnings("unchecked")
	public <T> MetaIdField<T> processId(DboTableMeta t, Field field, MetaAbstractClass<T> metaClass) {

		Method idMethod = getIdMethod(field);
		
		NoSqlId idAnno = field.getAnnotation(NoSqlId.class);
		MetaIdField<T> metaField = idMetaProvider.get();
		KeyGenerator gen = null;
		if(idAnno.usegenerator()) {
			Class<? extends KeyGenerator> generation = idAnno.generation();
			gen = ReflectionUtil.create(generation);
		}
		
		Class<?> type = field.getType();
		Converter converter = null;
		if(!NoConversion.class.isAssignableFrom(idAnno.customConverter()))
			converter = ReflectionUtil.create(idAnno.customConverter());
		
		String columnName = field.getName();
		if(!"".equals(idAnno.columnName()))
			columnName = idAnno.columnName();
		
		boolean isIndexed = false;
		if(field.isAnnotationPresent(NoSqlIndexed.class))
			isIndexed = true;
		
		if(field.isAnnotationPresent(NoSqlPartitionByThisField.class))
			throw new IllegalArgumentException("Field="+field+" is a primary key so it cannot have annotation="+NoSqlPartitionByThisField.class.getName());
		
		converter = lookupConverter(field, type, converter);
		if(converter == null)
			throw throwInvalidConverter(field);
		IdInfo info = new IdInfo();
		info.setIdMethod(idMethod);
		info.setConverter(converter);
		info.setGen(gen);
		info.setUseGenerator(idAnno.usegenerator());
		info.setMetaClass(metaClass);
		metaField.setup(t, info, field, columnName, isIndexed);
		return metaField;
	}

	private IllegalArgumentException throwInvalidConverter(Field field) {
		Class type = field.getType();
		if(Date.class.equals(type) || Calendar.class.equals(type))
			return new IllegalArgumentException("See this url for what you did wrong: https://github.com/deanhiller/playorm/wiki/Date-and-Calendar-Support");
		
		//TODO: create url with information on this one..
		return new IllegalArgumentException("No converter found for field='"+field.getName()+"' in class="
				+field.getDeclaringClass()+".  You need to either add one of the @*ToOne annotations, @Embedded, @Transient " +
						"or add your own converter calling EntityMgrFactory.setup(Map<Class, Converter>) which " +
						"will then work for all fields of that type OR add @NoSqlConverter(converter=YourConverter.class)" +
						" or finally if we missed a standard converter, we need to add it in file InspectorField.java" +
						" in the constructor and it is trivial code(and we can copy the existing pattern)");
	}

	private Method getIdMethod(Field field) {
		String name = field.getName();
		String newName = name.substring(0,1).toUpperCase() + name.substring(1);
		String methodName = "get"+newName; 
		
		Class<?> declaringClass = field.getDeclaringClass();
		try {
			Method method = declaringClass.getDeclaredMethod(methodName);
			if(!method.getReturnType().equals(field.getType()))
				throw new IllegalArgumentException("The method="+declaringClass.getName()+"."+methodName+" must" +
						" return the type="+field.getType().getName()+" but instead returns="+method.getReturnType().getName());
			
			return method;
		} catch (SecurityException e) {
			throw new RuntimeException("security issue on looking up method="+declaringClass.getName()+"."+methodName, e);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException("You are missing a method "+declaringClass.getName()+"."+methodName
					+"  This method exists as when you call it on a proxy, we make sure we do NOT hit the database" +
					" and instead just return the id that is inside the proxy.  Without this, we can't tell the" +
					" difference between a call to getName(where we have to hit the db and fill the proxy in) and" +
					" a call to just getting the id", e);
		}
	}

	public MetaField processColumn(DboTableMeta t, Field field) {
		NoSqlColumn col = field.getAnnotation(NoSqlColumn.class);
		MetaCommonField metaField = metaProvider.get();
		String colName = field.getName();
		if(col != null) {
			if(!"".equals(col.columnName()))
				colName = col.columnName();
		}

		boolean isIndexed = false;
		if(field.isAnnotationPresent(NoSqlIndexed.class))
			isIndexed = true;
		
		boolean isPartitioned = false;
		if(field.isAnnotationPresent(NoSqlPartitionByThisField.class))
			isPartitioned = true;
			
		Class<?> type = field.getType();
		Converter converter = null;
		if(col != null && !NoConversion.class.isAssignableFrom(col.customConverter()))
			converter = ReflectionUtil.create(col.customConverter());

		converter = lookupConverter(field, type, converter);
		if(converter == null)
			throw throwInvalidConverter(field);
		metaField.setup(t, field, colName, converter, isIndexed, isPartitioned);
		return metaField;			
	}
	
	private Converter lookupConverter(Field field, Class<?> type, Converter custom) {
		NoSqlConverter customConv = field.getAnnotation(NoSqlConverter.class);
		if(customConv != null) {
			Class<? extends Converter> convClazz = customConv.converter();
			return ReflectionUtil.create(convClazz);
		} else if(custom != null) {
			return custom;
		} else if(customConverters.get(type) != null) {
			return customConverters.get(type);
		} else if(StandardConverters.get(type) != null){
			return StandardConverters.get(type);
		}
		return null;
	}

	public void setCustomConverters(Map<Class, Converter> converters) {
		if(converters == null)
			return; //nothing to do
		
		this.customConverters = converters;
	}

	public MetaField processManyToOne(DboTableMeta t, Field field) {
		NoSqlManyToOne annotation = field.getAnnotation(NoSqlManyToOne.class);
		String colName = annotation.columnName();
		return processToOne(t, field, colName);
	}

	public MetaField processOneToOne(DboTableMeta t, Field field) {
		NoSqlOneToOne annotation = field.getAnnotation(NoSqlOneToOne.class);
		String colName = annotation.columnName();
		
		return processToOne(t, field, colName);
	}
	
	public MetaField processManyToMany(MetaClassSingle<?> metaClass, DboTableMeta t, Field field) {
		NoSqlManyToMany annotation = field.getAnnotation(NoSqlManyToMany.class);
		String colName = annotation.columnName();
		String keyFieldForMap = annotation.keyFieldForMap();
		
		return processToManyRelationship(metaClass, t, field, colName, keyFieldForMap);		
	}
	
	public MetaField processOneToMany(MetaClassSingle<?> ownerMeta, DboTableMeta t, Field field) {
		NoSqlOneToMany annotation = field.getAnnotation(NoSqlOneToMany.class);
		String colName = annotation.columnName();
		String keyFieldForMap = annotation.keyFieldForMap();
		
		return processToManyRelationship(ownerMeta, t, field, colName, keyFieldForMap);
	}

	private MetaField processToManyRelationship(MetaClassSingle<?> metaClass, DboTableMeta t, Field field, String colNameOrig,
			String keyFieldForMap) {
		String colName = field.getName();
		if(!"".equals(colNameOrig))
			colName = colNameOrig;
		
		if(field.isAnnotationPresent(NoSqlPartitionByThisField.class))
			throw new IllegalArgumentException("Field="+field+" is ToMany annotation so it cannot have annotation="+NoSqlPartitionByThisField.class.getName());
		
		Field fieldForKey = null;

		ParameterizedType genType = (ParameterizedType) field.getGenericType();
		Class entityType;
		if(field.getType().equals(Map.class)) {
			if("".equals(keyFieldForMap))
				throw new RuntimeException("Field="+field+" is a Map so @OneToMany annotation REQUIRES a keyFieldForMap attribute which is the field name in the child entity to use as the key");
			
			entityType = (Class) genType.getActualTypeArguments()[1];
			
			String fieldName = keyFieldForMap;
			
			try {
				fieldForKey = entityType.getDeclaredField(fieldName);
				fieldForKey.setAccessible(true);
			} catch (NoSuchFieldException e) {
				throw new RuntimeException("The annotation OneToMany on field="+field+" references a field="+fieldName+" that does not exist on entity="+entityType.getName());
			} catch (SecurityException e) {
				throw new RuntimeException(e);
			}
		} else {
			entityType = (Class) genType.getActualTypeArguments()[0];
		}
		
		return processToMany(metaClass, t, field, colName, entityType, fieldForKey);
	}
	
	@SuppressWarnings("unchecked")
	private MetaField processToMany(MetaClassSingle<?> ownerMeta, DboTableMeta t, Field field, String colName, Class entityType, Field fieldForKey) {
		//at this point we only need to verify that 
		//the class referred has the @NoSqlEntity tag so it is picked up by scanner at a later time
		Class<?> theSuperclass = null;
		Class<?> type = entityType;
		//at this point we only need to verify that 
		//the class referred has the @NoSqlEntity tag so it is picked up by scanner at a later time
		if(!entityType.isAnnotationPresent(NoSqlEntity.class)) {
			if(!entityType.isAnnotationPresent(NoSqlDiscriminatorColumn.class))
				throw new RuntimeException("type="+entityType+" needs the NoSqlEntity annotation(or a NoSqlDiscriminatorColumn if it is a subclass of an entity)" +
					" since field has *ToOne annotation.  field="+field.getDeclaringClass().getName()+"."+field.getName());
			theSuperclass = findSuperclassWithNoSqlEntity(entityType);
			type = theSuperclass;
			runOtherSuperclassChecks(entityType, field, theSuperclass);		
//			throw new RuntimeException("You have entityType="+entityType.getName()+" so that class needs the NoSqlEntity annotation" +
//					" since field has OneToMany annotation.  field="+field.getDeclaringClass().getName()+"."+field.getName()+" (or your wrote in the wrong entityType??)");
		}
		
		MetaAbstractClass<?> fkMeta = metaInfo.findOrCreate(type);
		
		if(theSuperclass != null) {
			//we need to swap the classMeta to the more specific class meta which may have not been 
			//created yet, oh joy...so we findOrCreate and the shell will be filled in when processing
			//that @NoSqlEntity when it scans the subclasses.
			MetaClassInheritance meta = (MetaClassInheritance) fkMeta;
			fkMeta = meta.findOrCreate(entityType, theSuperclass);
		}
		
		if(field.getType().equals(CursorToMany.class)) {
			MetaCursorField metaField = metaCursorProvider.get();
			metaField.setup(t, field, colName, ownerMeta, fkMeta);
			return metaField;
		}
		//field's type must be Map or List right now today
		if(!field.getType().equals(Map.class) && !field.getType().equals(List.class)
				&& !field.getType().equals(Set.class) && !field.getType().equals(Collection.class))
			throw new RuntimeException("field="+field+" must be Set, Collection, List or Map since it is annotated with OneToMany");

		MetaToManyField metaField = metaListProvider.get();
		metaField.setup(t, field, colName,  fkMeta, fieldForKey);
		
		return metaField;
	}

	@SuppressWarnings("unchecked")
	public MetaField processEmbedded(DboTableMeta t, Field field) {
		NoSqlEmbedded embedded = field.getAnnotation(NoSqlEmbedded.class);
		Class<?> type = field.getType();
		Class<?> valType = null;
		if(type.equals(List.class) || type.equals(Set.class)) {
			ParameterizedType genType = (ParameterizedType) field.getGenericType();
			type = (Class<?>) genType.getActualTypeArguments()[0];
		} else if(type.equals(Map.class)) {
			ParameterizedType genType = (ParameterizedType) field.getGenericType();
			type = (Class<?>) genType.getActualTypeArguments()[0];
			valType = (Class<?>) genType.getActualTypeArguments()[1];
		}
		
		String colNameOrig = embedded.columnNamePrefix();
		String colName = field.getName();
		if(!"".equals(colNameOrig))
			colName = colNameOrig;
		
		MetaField metaField;
		if(type.isAnnotationPresent(NoSqlEmbeddable.class)) {
			MetaAbstractClass<?> fkMeta = metaInfo.findOrCreate(type);
			MetaEmbeddedEntity temp = metaEmbeddedProvider.get();
			temp.setup(t, field, colName, fkMeta);
			metaField = temp;
		} else {
			Converter converter = lookupConverter(field, type);
			Converter valConverter = null;
			if(valType != null) 
				valConverter = lookupConverter(field, valType);
			
			MetaEmbeddedSimple meta = metaEmbeddedSimpleProvider.get();
			meta.setup(t, field, colName, converter, valConverter, type, valType);
			metaField = meta;
		}
		
		return metaField;
	}

	private Converter lookupConverter(Field field, Class<?> type) {
		Converter converter = lookupConverter(field, type, null);
		if(converter == null)
			throw new IllegalArgumentException("We found no converters(customer or standard for type="
					+type.getSimpleName()+" and this class is not annotated with " +
							"@NoSqlEmbeddable either.  The field that caused this issue is field="+field);
		return converter;
	}
	
	@SuppressWarnings("unchecked")
	public MetaField processToOne(DboTableMeta t, Field field, String colNameOrig) {
		String colName = field.getName();
		if(!"".equals(colNameOrig))
			colName = colNameOrig;

		boolean isIndexed = false;
		if(field.isAnnotationPresent(NoSqlIndexed.class))
			isIndexed = true;
		
		boolean isPartitionedBy = false;
		if(field.isAnnotationPresent(NoSqlPartitionByThisField.class))
			isPartitionedBy = true;
		
		Class<?> theSuperclass = null;
		Class<?> type = field.getType();
		//at this point we only need to verify that 
		//the class referred has the @NoSqlEntity tag so it is picked up by scanner at a later time
		if(!field.getType().isAnnotationPresent(NoSqlEntity.class) && 
				field.getType() != ToOneProvider.class) {
			if(!field.getType().isAnnotationPresent(NoSqlDiscriminatorColumn.class))
				throw new RuntimeException("type="+field.getType()+" needs the NoSqlEntity annotation(or a NoSqlDiscriminatorColumn if it is a subclass of an entity)" +
					" since field has *ToOne annotation.  field="+field.getDeclaringClass().getName()+"."+field.getName());
			theSuperclass = findSuperclassWithNoSqlEntity(field.getType());
			type = theSuperclass;
			runOtherSuperclassChecks(field.getType(), field, theSuperclass);
			
		} else if(field.getType().isAnnotationPresent(NoSqlInheritance.class)){
			throw new IllegalArgumentException("Okay, so here is the deal.  You have a ToOne relationship defined with an " +
					"Abstract class that has N number of subclasses.  I do not know which subclass to create a " +
					"proxy for unless I read yet another row in, BUT you may not want the extra hit so instead, " +
					"you MUST change this field to javax.inject.Provider instead so you can call provider.get() " +
					"at which point I will go to the nosql database and read the row in and the type information " +
					"and create the correct object for this type.  This is a special case, sorry about that.  In summary, all you " +
					"need to do is change this Field="+field+" to='private ToOneProvider<YourType> provider = new ToOneProvider<YourType>() " +
					"and then your getter should just be return provider.get() and the setter should be provider.set(yourInst) and all" +
					"will be fine with the world");
		} else if(field.getType() == ToOneProvider.class) {
			ParameterizedType genType = (ParameterizedType) field.getGenericType();
			type = (Class) genType.getActualTypeArguments()[0];
		}
		
		MetaProxyField metaField = metaProxyProvider.get();
		MetaAbstractClass<?> classMeta = metaInfo.findOrCreate(type);
		
		if(theSuperclass != null) {
			//we need to swap the classMeta to the more specific class meta which may have not been 
			//created yet, oh joy...so we findOrCreate and the shell will be filled in when processing
			//that @NoSqlEntity when it scans the subclasses.
			MetaClassInheritance meta = (MetaClassInheritance) classMeta;
			classMeta = meta.findOrCreate(field.getType(), theSuperclass);
		}
		
		metaField.setup(t, field, colName, classMeta, isIndexed, isPartitionedBy);
		return metaField;
	}

	private void runOtherSuperclassChecks(Class entityType, Field field, Class<?> theSuperclass) {
		if(log.isDebugEnabled())
			log.debug("superclass with @NoSqlEntity="+theSuperclass);
		if(theSuperclass == null)
			throw new RuntimeException("type="+entityType+" has a NoSqlDiscriminatorColumn but as we go " +
					"up the superclass tree, none of the classes are annotated with NoSqlEntity, please add that annotation");
		NoSqlInheritance anno = theSuperclass.getAnnotation(NoSqlInheritance.class);
		if(anno == null)
			throw new RuntimeException("type="+entityType+" has a NoSqlDiscriminatorColumn but as we go " +
					"up the superclass tree, none of the classes are annotated with NoSqlInheritance");
		else if(!classExistsInList(anno, entityType))
			throw new RuntimeException("type="+entityType+" has a NoSqlDiscriminatorColumn and has a super class with NoSqlEntity and NoSqlInheritance but is not listed" +
					"in the NoSqlInheritance tag as one of the subclasses to scan.  Please add it");
	}

	@SuppressWarnings("unused")
	private boolean classExistsInList(NoSqlInheritance anno, Class<?> class1) {
		Class[] subclasses = anno.subclassesToScan();
		Class c = null;
		for(Class sub : subclasses) {
			if(sub == class1)
				return true;
		}		
		
		return false;
	}

	private Class findSuperclassWithNoSqlEntity(Class<?> type) {
		if(type.isAnnotationPresent(NoSqlEntity.class))
			return type;
		else if(type == Object.class)
			return null;
		return findSuperclassWithNoSqlEntity(type.getSuperclass());
	}
}
