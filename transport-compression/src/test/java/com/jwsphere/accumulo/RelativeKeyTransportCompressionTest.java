package com.jwsphere.accumulo;

import org.junit.Test;

public class RelativeKeyTransportCompressionTest {

	@Test
	public void roundtrip() {
		TransportCompressionTest.roundtrip(new RelativeKeyTransportCompression());
	}
	
}
