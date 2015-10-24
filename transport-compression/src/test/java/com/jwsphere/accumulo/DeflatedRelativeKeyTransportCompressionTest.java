package com.jwsphere.accumulo;

import org.junit.Test;

public class DeflatedRelativeKeyTransportCompressionTest {

	@Test
	public void roundtrip() {
		TransportCompressionTest.roundtrip(new DeflatedRelativeKeyTransportCompression());
	}

}
