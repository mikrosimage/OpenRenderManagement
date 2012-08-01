package fr.mikrosimage.puli.ui.client.data;

import java.util.ArrayList;
import java.util.List;

import com.google.gwt.json.client.JSONArray;
import com.google.gwt.json.client.JSONObject;
import com.google.gwt.json.client.JSONValue;

public class PuliTranslator {

	public static JSONValue getValue(JSONObject object, String key) {
		JSONValue jsonValue = null;
		try {
			jsonValue = object.get(key);
		} catch (Exception e) {
			jsonValue = null;
		}
		return jsonValue;
	}

	public static PuliNode translateNode(JSONObject jsonObject) {
		PuliNode puliNode = new PuliNode();
		JSONValue completionValue = jsonObject.get(PuliNode.COMPLETION_FIELD);
		JSONValue idValue = jsonObject.get(PuliNode.ID_FIELD);
		JSONValue nameValue = jsonObject.get(PuliNode.NAME_FIELD);
		JSONValue statusValue = jsonObject.get(PuliNode.STATUS_FIELD);
		if ( completionValue != null && completionValue.isNumber() != null) {
				puliNode.setCompletion(completionValue.isNumber().doubleValue());
		}
		if (idValue != null && idValue.isNumber() != null) {
			puliNode.setId(Long.valueOf((long) idValue.isNumber().doubleValue()));
		}
		if (nameValue != null && nameValue.isString() != null) {
			puliNode.setName(nameValue.isString().stringValue());
		}
		if (statusValue != null && statusValue.isNumber() != null) {
			puliNode.setStatus((int) statusValue.isNumber().doubleValue());
		}
		return puliNode;
	}

	public static List<PuliNode> translateNodes(JSONArray jsonArray) {
		List<PuliNode> nodes = new ArrayList<PuliNode>();
		for (int i = 0; i < jsonArray.size(); i++) {
			JSONValue jsonValue = jsonArray.get(i);
			JSONObject object = jsonValue.isObject();
			PuliNode node = translateNode(object);
			nodes.add(node);
		}
		return nodes;
	}

}
