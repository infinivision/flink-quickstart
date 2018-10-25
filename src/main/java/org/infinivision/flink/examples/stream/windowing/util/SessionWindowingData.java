
package org.infinivision.flink.examples.stream.windowing.util;

/**
 * Data for SessionWindowingITCase.
 */
public class SessionWindowingData {

	public static final String EXPECTED = "(a,1,1)\n" + "(c,6,1)\n" + "(c,11,1)\n" + "(b,1,3)\n" +
			"(a,10,1)";

	private SessionWindowingData() {
	}
}
