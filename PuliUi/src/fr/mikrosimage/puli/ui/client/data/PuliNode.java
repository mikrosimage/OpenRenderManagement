package fr.mikrosimage.puli.ui.client.data;

import java.util.HashMap;
import java.util.Map;

public class PuliNode {
	public static final String ID_FIELD = "id";
	public static final String NAME_FIELD = "name";
	public static final String COMPLETION_FIELD = "completion";
	public static final String STATUS_FIELD = "status";

	private final Map<String, Object> maps = new HashMap<String, Object>();

	public Long getId() {
		return (Long) get(ID_FIELD);
	}

	public String getName() {
		return (String) get(NAME_FIELD);
	}

	public double getCompletion() {
		return (Double) get(COMPLETION_FIELD);
	}

	public int getStatus() {
		return (Integer) get(STATUS_FIELD);
	}

	public void setId(Long id) {
		set(ID_FIELD, id);
	}

	public void setName(String name) {
		set(NAME_FIELD, name);
	}

	public void setCompletion(double completion) {
		set(COMPLETION_FIELD, Double.valueOf(completion));
	}

	public void setStatus(int status) {
		set(STATUS_FIELD, Integer.valueOf(status));
	}
	
	public void set(String key, Object value) {
		maps.put(key, value);
	}

	public Object get(String property) {
		return maps.get(property);
	}
}
