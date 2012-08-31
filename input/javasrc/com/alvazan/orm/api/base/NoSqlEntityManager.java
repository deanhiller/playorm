package com.alvazan.orm.api.base;

import java.util.List;

import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.layer0.base.BaseEntityManagerImpl;
import com.google.inject.ImplementedBy;

@ImplementedBy(BaseEntityManagerImpl.class)
public interface NoSqlEntityManager {

	/**
	 * Retrieve underlying interface to write raw columns to.  This works the same as the NoSqlEntityManager
	 * in that you must call flush to execute all the calls to persist, but is the raw interface.
	 * 
	 * @return 
	 */
	public NoSqlSession getSession();

	/**
	 * Retrieve a special interface that deals with rows and still does indexing when you persist/remove rows.  This interface is
	 * used when inserting unknown datasets into a nosql store where you want indexing to be automatic still.  Generally used
	 * for research datasets where a user uploading data is telling you the columns and what columns to index for him.
	 */
	public NoSqlTypedSession getTypedSession();
	
	/**
	 * Creates a 'Remove' action in the write cache that will be sent to nosql store when flush is called.  This 
	 * method also creates RemoveIndex actions that will be sent when flush is called as well. 
	 * @param entity
	 */
	public void remove(Object entity);
	
	/**
	 * Creates a 'Persist' action in the write cache that will be sent to nosql store when flush is called.  This method
	 * also creates PersistIndex and RemoveIndex actions if you have indexed fields and the index needs to be modified
	 * and those are sent when flush is called as well.
	 * @param entity
	 */
	public void put(Object entity);
	
	//public void putAll(List<Object> entities);
	
	/**
	 * This is NOSql so do NOT use find in a loop!!!!  Use findAll instead and then loop over the items.
	 * If your network latency is 5 ms, looking up 1000 records will cost you 5 seconds in a loop(plus processing time)
	 * where findAll will cost you 5 ms (plus processing time).  ie. findAll is better for looking up lots of entities!!
	 * 
	 * @param entityType
	 * @param key
	 * @return
	 */
	public <T> T find(Class<T> entityType, Object key);
	
	/**
	 * An efficient operation in nosql for retrieving many entities at once.  This is the operation
	 * we use very frequently in the ORM for OneToMany operations so we can fetch all your relations 
	 * extremely fast(as they are fetched in parallel not series so 5ms network latency for 1000 objects
	 * is not 5 seconds but just 5ms as it is done in parallel).  
	 * 
	 * @param entityType
	 * @param keys
	 * @return
	 */
	public <T> List<KeyValue<T>> findAllList(Class<T> entityType, List<? extends Object> keys);
	
	/** 
	 * @param entityType
	 * @param keys
	 * @return
	 */
	public <T> Cursor<KeyValue<T>> findAll(Class<T> entityType, Iterable<? extends Object> keys);
	
	/**
	 * Just like hibernate getReference call.  Use this when you have an id of an object and
	 * have another object like User and you want to call User.addAccount(Account account).  First
	 * get a fake account with Account account = mgr.getReference(Account.class, accountId) and
	 * then set the fake account into the User object and save the user object.  The User is now
	 * related to the account with that accountId and you did not have to hit the database to
	 * read in the account.  Again, this is the same as JPA getReference method.
	 * @param entityType
	 * @param key
	 * @return
	 */
	public <T> T getReference(Class<T> entityType, Object key);
	
	/**
	 * Unlike RDBMS, there are no transactions, BUT all the calls to putAll and put are cached
	 * in-memory until flush is called.  This allows us to easily queue up all writes to
	 * the datastore and 
	 * <ol>
	 *    <ul> Send all writes as one to incur less i/o over the network
	 *    <ul> not block your thread for every put/putAll that is called(only block on writing all during flush)
	 *    <ul> time how long all the writes take.  
	 * </ol>
	 * 
	 * It is more likely to keep things more consistent
	 */
	public void flush();
	
	public <T> Query<T> createNamedQuery(Class<T> forEntity, String namedQuery);
	
	/**
	 * In certain cases where you have a bi-directional association, you need a primary key in
	 * the children before you can save the parent.  ie. If you have an Account that has a list
	 * of Activity and an Activity with an Account, the proper way to code this is such
	 * 
	 * <ol>
	 *    <li>Account acc = new Account() </li>
	 *    <li>entityManager.fillInWithKey(acc) </li>
	 *    <li>Activity act1 = new Activity() </li>
	 *    <li>act1.setAccount(acc) </li>
	 *    <li>entityManager.put(act1) //act1 key is generated and filled in here</li>
	 *    <li>acc.addActivity(act1) //here account has an act1 WITH a key</li>
	 *    <li>entityManager.put(acc) </li>
	 *  </ol>
	 *  
	 * @param acc
	 */
	public void fillInWithKey(Object acc);
	
	/**
	 * Mainly for framework code but a nice way to get the key of an unknown entity
	 * where you don't care about the entity but just need that dang key
	 * @param entity
	 * @return
	 */
	public Object getKey(Object entity);

	/**
	 * This is a convenience method for in memory database and cassandra database
	 * so that you can run unit tests against in-memory (and run the same unit tests
	 * against cassandra live as well).
	 * 
	 * @param recreateMeta - It is usually a good idea to recreate the meta objects in
	 * the database after clearing it as the more raw layers depend on the meta.  The ORM
	 * itself created the original meta so it really doesn't care about the meta in the
	 * database
	 */
	public void clearDatabase(boolean recreateMeta);
}
