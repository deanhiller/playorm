package com.alvazan.orm.api.base.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;

public class NoSql {

	private static final Logger log = LoggerFactory.getLogger(NoSql.class);
	private static NoSqlEntityManagerFactory factory;
	
	private static ThreadLocal<State> entityManager = new ThreadLocal<State>();
	private static Class testClass;
	private static PlayCallback playCallback;
	
	@SuppressWarnings("rawtypes")
	public static void initialize(PlayCallback callback) {
		if(factory != null)
			return;
		
		log.info("Factory is null so we are intializing it");
			
		playCallback = callback;
		List<Class> list = callback.getClassesToScan();
		ClassLoader cl = callback.getClassLoader();
		testClass = list.get(0);
		
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(Bootstrap.AUTO_CREATE_KEY, "create");
		props.put(Bootstrap.LIST_OF_EXTRA_CLASSES_TO_SCAN_KEY, list);
		NoSqlEntityManagerFactory f = Bootstrap.create(DbTypeEnum.IN_MEMORY, props, null, cl);
		factory = f;
	} // initialize
	
	@SuppressWarnings("rawtypes")
	private static void testForRescan() {
		List<Class> classesToScan = playCallback.getClassesToScan();
		Class newClass = classesToScan.get(0);
		if(newClass == testClass)
			return;
		
		//otherwise, we need a rescan of all the new classes
		ClassLoader cl = playCallback.getClassLoader();
		factory.rescan(classesToScan, cl);
		testClass = newClass;
	}

	/**
	 * Returns the same entity manager if you are in the same request scope so you keep getting the same entitymanager for a request
	 * @return
	 */
	public static NoSqlEntityManager em() {
		testForRescan();
		Object request = playCallback.getCurrentRequest();
		NoSqlEntityManagerFactory f = factory;
		State state = entityManager.get();
		if(state == null) {
			return createNewEm(request);
		} else if(state.getRequest() == request) {
			return state.getEm();
		}
		//otherwise this is a new request needing a new em 
		return createNewEm(request);
	} // em

	private static NoSqlEntityManager createNewEm(Object request) {
		State state;
		state = new State();
		state.setRequest(request);
		state.setEm(factory.createEntityManager());
		entityManager.set(state);
		return state.getEm();
	}

	private static class State {
		private NoSqlEntityManager em;
		private Object request;
		
		public NoSqlEntityManager getEm() {
			return em;
		}
		public void setEm(NoSqlEntityManager em) {
			this.em = em;
		}
		public Object getRequest() {
			return request;
		}
		public void setRequest(Object request) {
			this.request = request;
		}
		
	}

}
