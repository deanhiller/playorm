package com.alvazan.orm.layer3.spi.index;

import java.util.List;
import java.util.Map;

public interface IndexReaderWriter {
	
	String IDKEY = "id";
	
	void sendRemoves(Map<String, List<? extends IndexRemove>> removeFromIndex);

	void sendAdds(Map<String, List<IndexAdd>> addToIndex);

}
