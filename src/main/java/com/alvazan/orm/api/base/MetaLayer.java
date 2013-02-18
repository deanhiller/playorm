package com.alvazan.orm.api.base;

public interface MetaLayer {

	/**
	 * Mainly for framework code but a nice way to get the key of an unknown entity
	 * where you don't care about the entity but just need that dang key
	 * @param entity
	 * @return The desired rowkey
	 */
	public Object getKey(Object entity);

	/**
	 * Determining if an Entity is managed by this manager is not just a test of whether @Entity annotation exists as there
	 * are cases like inheritance where subclasses do not have that annotation but are managed by the ORM.
	 * @param type
	 * @return true if the entity is managed by this Manager
	 */
	public boolean isManagedEntity(Class<?> type);
	
	public String getKeyFieldName(Class<?> type);
	
	/**
	 * Takes the id in String form and convert it to the correct id form whatever that may be
	 * as far as Long, String, etc. etc.
	 * 
	 * @param entityType
	 * @return converted Id
	 */
	public Object convertIdFromString(Class<?> entityType, String idAsString);
	
}
