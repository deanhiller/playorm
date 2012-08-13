package com.alvazan.orm.api.base;

import java.util.List;

import com.alvazan.orm.api.spi2.KeyValue;
import com.alvazan.orm.api.spi2.NoSqlSession;
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
	
	/**
	 * Use put(Object entity) in a for loop instead BECAUSE changes are not sent to nosql store until the flush anyways.
	 * putAll will go away in future release
	 * @param entities
	 * @deprecated Use put(Object entity) in a loop and flush will send all puts down at one time.
	 */
	@Deprecated
	public void putAll(List<Object> entities);
	
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
	 * Very efficient operation in nosql for retrieving many entities at once.  This is the operation
	 * we use very frequently in the ORM for OneToMany operations so we can fetch all your relations 
	 * extremely fast(as they are fetched in parallel not series so 5ms network latency for 1000 objects
	 * is not 5 seconds but just 5ms as it is done in parallel).
	 * 
	 * @param entityType
	 * @param keys
	 * @return
	 */
	public <T> List<KeyValue<T>> findAll(Class<T> entityType, List<? extends Object> keys);
	
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
	
	/**
	 * Not ready for use as of yet.  
	 * 
	 * Best explained with an example.  Let's say you have a table with 1 billion rows
	 * and let's say you have 1 million customers each with on avera 1000 rows in that
	 * table(ie. total 1 billion).  In that case, it would be good to create 1 million
	 * indexes with 1000 nodes in them each rather than one 1 billion node index as it
	 * would be 1000's of times faster to fetch the small index and query it.  Pretend
	 * the entity representing this 1 billion row table was ActionsTaken.class, then to
	 * get the index for all rows relating to a user that you can then query you would
	 * call entityMgr.getIndex(ActionsTaken.class, "/byUser/"+user.getId());
	 * 
	 * Going on, let's say that same 1 billion activity table is also related to 500k
	 * commentors.  You may have another 500k indexes for querying when you have the 
	 * one commentor and want to search on other stuff so you would get the index to
	 * query like so entityMgr.getIndex(ActionsTaken.class, "/byCommentor/"+commentor.getId());
	 * 
	 * In this methodology you are just breaking up HUGE tables into small subtables that
	 * can be queried.
	 * 
	 * @param forEntity
	 * @param indexName
	 * @return
	 */
	public <T> Index<T> getIndex(Class<T> forEntity, String indexName);

	public <T> Index<T> getIndex(Class<T> forEntity);
	
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
	 */
	public void clearDatabase();
}
