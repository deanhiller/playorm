package com.alvazan.orm.layer3.spi.index;

import java.util.List;
import java.util.Map;


public interface IndexReaderWriter {
	
	String IDKEY = "id";
	
	void sendRemoves(Map<String, List<? extends IndexRemove>> removeFromIndex);

	void sendAdds(Map<String, List<IndexAdd>> addToIndex);

	/**
	 * On startup, we will add named queries by calling this method so you can return us
	 * a factory.  This factory returns us a Query object we will set parameters on to
	 * run the query
	 * @param classMeta 
	 */
	@SuppressWarnings("rawtypes")
	SpiMetaQuery createQueryFactory();
}
