package com.alvazan.play;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import play.data.binding.BeanWrapper;
import play.data.binding.Binder;
import play.data.binding.ParamNode;
import play.data.binding.ParamNode.RemovedNode;
import play.data.validation.Validation;
import play.exceptions.UnexpectedException;

import com.alvazan.orm.api.base.CursorToMany;
import com.alvazan.orm.api.base.MetaLayer;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.anno.NoSqlManyToMany;
import com.alvazan.orm.api.base.anno.NoSqlManyToOne;
import com.alvazan.orm.api.base.anno.NoSqlOneToMany;
import com.alvazan.orm.api.base.anno.NoSqlOneToOne;

public class NoSqlModel {
	
	private static final Logger log = LoggerFactory.getLogger(NoSqlModel.class);
	
    public static <T> T create(ParamNode rootParamNode, String name, Class<T> type, Annotation[] annotations) {
        try {
            Constructor<T> c = type.getDeclaredConstructor();
            c.setAccessible(true);
            T model = c.newInstance();
            return edit(rootParamNode, name, model, annotations);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static String retrieveValue(ParamNode id) {
        if(id == null)
        	return null;
        
        String[] ids = id.getValues();
        if(ids == null || ids.length == 0)
        	return null;
        
        if("".equals(ids[0]))
        	return null;
            
		return ids[0];
	}
    
    @SuppressWarnings("rawtypes")
	public static <T> T edit(ParamNode rootParamNode, String name, T o, Annotation[] annotations) {
        ParamNode paramNode = rootParamNode.getChild(name, true);
        // #1195 - Needs to keep track of whick keys we remove so that we can restore it before
        // returning from this method.
        List<ParamNode.RemovedNode> removedNodesList = new ArrayList<ParamNode.RemovedNode>();
        try {
            // Start with relations
            Set<Field> fields = new HashSet<Field>();
            Class clazz = o.getClass();
            while (!clazz.equals(Object.class)) {
                Collections.addAll(fields, clazz.getDeclaredFields());
                clazz = clazz.getSuperclass();
            }
            for (Field field : fields) {
            	processField(field, paramNode, removedNodesList, o);
            }
            ParamNode beanNode = rootParamNode.getChild(name, true);
            Binder.bindBean(beanNode, o, annotations);
            return (T) o;
        } catch (Exception e) {
            throw new UnexpectedException(e);
        } finally {
            // restoring changes to paramNode
            ParamNode.restoreRemovedChildren( removedNodesList );
        }
    }

	@SuppressWarnings("rawtypes")
	private static void processField(Field field, ParamNode paramNode, List<RemovedNode> removedNodesList, Object o) {
    	NoSqlEntityManager em = NoSql.em();
    	MetaLayer meta = em.getMeta();
    	
        boolean isEntity = false;
        Class<?> relation = null;
        boolean multiple = false;
        //
        if (field.isAnnotationPresent(NoSqlOneToOne.class) || field.isAnnotationPresent(NoSqlManyToOne.class)) {
            isEntity = true;
            relation = field.getType();
        } else if (field.isAnnotationPresent(NoSqlOneToMany.class) || field.isAnnotationPresent(NoSqlManyToMany.class)) {
            Class fieldType = (Class) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
            isEntity = true;
            relation = fieldType;
            multiple = true;
        }

        if (!isEntity)
        	return;

        ParamNode fieldParamNode = paramNode.getChild(field.getName(), true);

        String keyName = meta.getKeyFieldName(relation);
        ParamNode idChild = fieldParamNode.getChild(keyName, true);
        String theIdStr = retrieveValue(idChild);
        if (multiple && Collection.class.isAssignableFrom(field.getType())) {
//        	Collection l = new ArrayList();
//        	if (SortedSet.class.isAssignableFrom(field.getType())) {
//        		l = new TreeSet();
//        	} else if (Set.class.isAssignableFrom(field.getType())) {
//        		l = new HashSet();
//        	}
        	log.trace("not implemented");
        	//NOTE: for now we skip this
        	
        } else if (theIdStr != null) {
        	Object theId = meta.convertIdFromString(relation, theIdStr);
        	Object to = em.find(relation, theId);
        	if(to != null) {
        		edit(paramNode, field.getName(), to, field.getAnnotations());
        		// Remove it to prevent us from finding it again later
        		paramNode.removeChild( field.getName(), removedNodesList);
        		set(field, o, to);
        		return;
        	}
        	
    		Validation.addError(fieldParamNode.getOriginalKey(), "validation.notFound", theIdStr);
    		// Remove only the key to prevent us from finding it again later
    		// This how the old impl does it..
    		fieldParamNode.removeChild(keyName, removedNodesList);
    		if (fieldParamNode.getAllChildren().size()==0) {
    			// remove the whole node..
    			paramNode.removeChild( field.getName(), removedNodesList);
    		}

        } else {
        	set(field, o, null);
        	// Remove the key to prevent us from finding it again later
        	fieldParamNode.removeChild(keyName, removedNodesList);
        }
	}

	private static void set(Field field, Object o, Object to) {
		field.setAccessible(true);
		try {
			field.set(o, to);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
}
