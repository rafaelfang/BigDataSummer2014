package edu.missouri.hadoop;

//Halton sequence class
public class HaltonSequence {
	// bases
	static final int[] P = { 2, 3 };
	// maximum number of digits allowed
	static final int[] K = { 63, 40 };

	private long index;
	private double[] x;
	private double[][] q;
	private int[][] d;

	HaltonSequence(long startindex) {
		index = startindex;
		x = new double[K.length];
		q = new double[K.length][];
		d = new int[K.length][];
		for (int i = 0; i < K.length; i++) {
			q[i] = new double[K[i]];
			d[i] = new int[K[i]];
		}

		for (int i = 0; i < K.length; i++) {
			long k = index;
			x[i] = 0;

			for (int j = 0; j < K[i]; j++) {
				q[i][j] = (j == 0 ? 1.0 : q[i][j - 1]) / P[i];
				d[i][j] = (int) (k % P[i]);
				k = (k - d[i][j]) / P[i];
				x[i] += d[i][j] * q[i][j];
			}
		}
	}

	double[] nextPoint() {
		index++;
		for (int i = 0; i < K.length; i++) {
			for (int j = 0; j < K[i]; j++) {
				d[i][j]++;
				x[i] += q[i][j];
				if (d[i][j] < P[i]) {
					break;
				}
				d[i][j] = 0;
				x[i] -= (j == 0 ? 1.0 : q[i][j - 1]);
			}
		}
		return x;
	}
}
