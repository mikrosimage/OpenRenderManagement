package fr.mikrosimage.puli.ui.client;

import java.util.List;

import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.JavaScriptObject;
import com.google.gwt.core.client.Scheduler;
import com.google.gwt.json.client.JSONArray;
import com.google.gwt.json.client.JSONObject;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.RootPanel;

import fr.mikrosimage.puli.ui.client.data.PuliNode;
import fr.mikrosimage.puli.ui.client.data.PuliStatus;
import fr.mikrosimage.puli.ui.client.data.PuliTranslator;

public class PuliUi implements EntryPoint {
    private final PuliStatus puliStatus = new PuliStatus();

    @Override
    public void onModuleLoad() {
        // String url = "response.txt";
        String url = "http://localhost:8004/nodes/1/children?callback=";
        getJson(0, url, this);
        // RequestBuilder builder = new RequestBuilder(RequestBuilder.GET, URL
        // .encode(url));
        //
        // try {
        // builder.sendRequest(null, new RequestCallback() {
        // public void onError(Request request, Throwable exception) {
        // Window
        // .alert("Couldn't connect to server (could be timeout, SOP violation, etc.)");
        // }
        //
        // public void onResponseReceived(Request request,
        // Response response) {
        // int code = response.getStatusCode();
        // String responseText = response.getText();
        // if (Response.SC_OK == response.getStatusCode()) {
        // // Process the response in response.getText()
        // JSONValue parse = JSONParser.parse(response.getText());
        // JSONObject object = parse.isObject();
        // JSONArray array = object.get("children").isArray();
        // List<PuliNode> translateNodes = PuliTranslator
        // .translateNodes(array);
        // createTable(translateNodes);
        // } else {
        // Window.alert("status = " + response.getStatusCode() + ", text =" +
        // response.getStatusText());
        // }
        // }
        // });
        // } catch (RequestException e) {
        // Window.alert("Couldn't connect to server");
        // }
    }

    /**
     * Handle the response to the request for stock data from a remote server.
     */
    public void handleJsonResponse(JavaScriptObject jso) {
        if (jso == null) {
            Window.alert("Couldn't retrieve JSON");
            return;
        }
        JSONObject object = new JSONObject(jso);
        JSONArray array = object.get("children").isArray();
        List<PuliNode> translateNodes = PuliTranslator.translateNodes(array);
        createTable(translateNodes);
    }

    private void createTable(final List<PuliNode> translateNodes) {
        final String[] tableHeaders = new String[] { PuliNode.ID_FIELD, PuliNode.NAME_FIELD, PuliNode.COMPLETION_FIELD, PuliNode.STATUS_FIELD };
        final FlexTable flexTable = new FlexTable();
        flexTable.setCellPadding(0);
        flexTable.setCellSpacing(0);
        flexTable.setBorderWidth(1);
        for (int i = 0; i < tableHeaders.length; i++) {
            flexTable.setWidget(0, i, new Label(tableHeaders[i]));
        }
        flexTable.getRowFormatter().setStyleName(0, "hearders");
        Scheduler.get().scheduleIncremental(new Scheduler.RepeatingCommand() {
            private int index = 1;

            @Override
            public boolean execute() {
                PuliNode puliNode = translateNodes.get(index);
                for (int i = 0; i < tableHeaders.length; i++) {
                    Object object = puliNode.get(tableHeaders[i]);
                    String value = "";
                    if (object != null) {
                        value = object.toString();
                    }
                    if (PuliNode.COMPLETION_FIELD.equals(tableHeaders[i])) {
                        value = (puliNode.getCompletion() * 100) + "%";
                    }
                    if (PuliNode.STATUS_FIELD.equals(tableHeaders[i])) {
                        value = puliStatus.getStatus(puliNode.getStatus());
                    }
                    flexTable.setWidget(index, i, new Label(value));
                }
                index++;
                return index < translateNodes.size();
            }
        });
        RootPanel.get().add(flexTable);
    }

    /**
     * Make call to remote server.
     */
    public native static void getJson(int requestId, String url, PuliUi handler) /*-{
		var callback = "callback" + requestId;

		// [1] Create a script element.
		var script = document.createElement("script");
		script.setAttribute("src", url + callback);
		script.setAttribute("type", "text/javascript");

		// [2] Define the callback function on the window object.
		window[callback] = function(jsonObj) {
			// [3]
			handler.@fr.mikrosimage.puli.ui.client.PuliUi::handleJsonResponse(Lcom/google/gwt/core/client/JavaScriptObject;)(jsonObj);
			window[callback + "done"] = true;
		}

		// [4] JSON download has 1-second timeout.
		setTimeout(
				function() {
					if (!window[callback + "done"]) {
						handler.@fr.mikrosimage.puli.ui.client.PuliUi::handleJsonResponse(Lcom/google/gwt/core/client/JavaScriptObject;)(null);
					}

					// [5] Cleanup. Remove script and callback elements.
					document.body.removeChild(script);
					delete window[callback];
					delete window[callback + "done"];
				}, 20000);

		// [6] Attach the script element to the document body.
		document.body.appendChild(script);
    }-*/;
}
