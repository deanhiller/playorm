package com.alvazan.play;

import play.exceptions.JPAException;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;

public class NoSql {

    private static NoSqlEntityManagerFactory entityManagerFactory = null;
    private static ThreadLocal<NoSql> local = new ThreadLocal<NoSql>();
    private NoSqlEntityManager entityManager;

    static NoSql get() {
        if (local.get() == null) {
            throw new JPAException("The JPA context is not initialized. JPA Entity Manager automatically start when one or more classes annotated with the @NoSqlEntity annotation are found in the application.");
        }
        return local.get();
    }

    static void clearContext() {
        local.remove();
    }

    static void createContext(NoSqlEntityManager entityManager) {
        if (local.get() != null) {
//            try {
//                local.get().entityManager.close();
//            } catch (Exception e) {
//                // Let's it fail
//            }
            local.remove();
        }
        NoSql context = new NoSql();
        context.entityManager = entityManager;
        local.set(context);
    }

    // ~~~~~~~~~~~
    /*
     * Retrieve the current entityManager
     */
    public static NoSqlEntityManager em() {
        return get().entityManager;
    }

    /**
     * @return true if an entityManagerFactory has started
     */
    public static boolean isEnabled() {
        return entityManagerFactory != null;
    }

    /*
     * Build a new entityManager.
     * (In most case you want to use the local entityManager with em)
     */
    public static NoSqlEntityManager newEntityManager() {
        return entityManagerFactory.createEntityManager();
    }

	public static NoSqlEntityManagerFactory getEntityManagerFactory() {
		return entityManagerFactory;
	}

	static void setEntityManagerFactory(NoSqlEntityManagerFactory factory) {
		entityManagerFactory = factory;
	}

}
