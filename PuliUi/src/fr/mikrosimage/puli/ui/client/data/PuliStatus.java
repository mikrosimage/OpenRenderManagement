package fr.mikrosimage.puli.ui.client.data;

import java.util.HashMap;
import java.util.Map;

public class PuliStatus {

	private final Map<Integer, String> statusMap = new HashMap<Integer, String>();
	
	public PuliStatus() {
		statusMap.put(0, "BLOCKED");
		statusMap.put(1, "READY");
		statusMap.put(2, "RUNNING");
		statusMap.put(3, "DONE");
		statusMap.put(4, "ERROR");
		statusMap.put(5, "CANCELED");
		statusMap.put(6, "PAUSED");
	}
	
	public String getStatus(int status) {
		return statusMap.get(status);
	}
	
}
