package com.alvazan.orm.api.z8spi;



public interface CreateCfCallback {

	/** 
	 * For cassandra, def param is hte ColumnFamilyDefinition and you return that
	 * @param cfName
	 * @param def
	 * @return
	 */
	public Object modifyColumnFamily(String cfName, Object def);
	/** 
	 * For cassandra, def param is the KeyspaceDefinition and you return that
	 * @param cfName
	 * @param def
	 * @return
	 */
	public Object configureKeySpace(String keyspaceName, Object def);

}
