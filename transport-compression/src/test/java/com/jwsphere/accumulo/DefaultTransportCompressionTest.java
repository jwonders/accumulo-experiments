package com.jwsphere.accumulo;

import org.junit.Test;

public class DefaultTransportCompressionTest {

	@Test
	public void roundtrip() {
		TransportCompressionTest.roundtrip(new DefaultTransportCompression());
	}
	
}
