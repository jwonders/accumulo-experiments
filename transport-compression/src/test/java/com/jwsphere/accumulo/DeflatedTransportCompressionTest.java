package com.jwsphere.accumulo;

import org.junit.Test;

public class DeflatedTransportCompressionTest {

	@Test
	public void roundtrip() {
		TransportCompressionTest.roundtrip(new DeflateTransportCompression());
	}
	
}
