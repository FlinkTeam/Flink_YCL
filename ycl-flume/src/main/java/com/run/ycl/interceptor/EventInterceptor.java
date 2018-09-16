package com.run.ycl.interceptor;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventInterceptor implements Interceptor {

	private static final Logger LOGGER = LoggerFactory.getLogger(EventInterceptor.class);

	private Map<String, String> protocolMapping = null;

	@Override
	public void initialize() {
		protocolMapping = new HashMap<String, String>();
	}

	@Override
	public Event intercept(Event event) {
		try {
			Map<String, String> headers = event.getHeaders();

			String bcpFile = headers.get("bcp_Name");
			LOGGER.debug("bcpFile================{}", bcpFile);

			//137-110000-1530883018-00001-WA_MFORENSICS_010100-0.bcp
			String[] nameStr = bcpFile.split("-");
			String sourceName = nameStr[4].toUpperCase();

			String xmlFile = headers.get("");

			//headers.put(AmalaConstants.HEADER_FLUME_DATA_CATEGORY, mappingType);

			return event;
		} catch (Exception e) {
			LOGGER.error("intercept event has error!", e);
		}
		return null;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		return null;
	}

	@Override
	public void close() {

	}
	/*

	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> out = Lists.newArrayList();
		for (Event event : events) {
			Event outEvent = intercept(event);
			if (outEvent != null) {
				out.add(outEvent);
			}
		}
		return out;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	public static class Builder implements Interceptor.Builder {

		@Override
		public Interceptor build() {
			return new EventInterceptor();
		}

		@Override
		public void configure(Context context) {
		}

	}
*/
}
