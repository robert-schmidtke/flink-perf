package com.github.projectflink.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


public class Grep {

	public static void main(String[] args) {
		long start = System.nanoTime();
		
		String master = args[0];
		String inFile = args[1];
		String outFile = args[2];

		String patterns[] = new String[args.length-3];
		System.arraycopy(args,3,patterns,0,args.length-3);
		System.err.println("Starting spark with master="+master+" in="+inFile);
		System.err.println("Using patterns: "+ Arrays.toString(patterns));

		SparkConf conf = new SparkConf().setAppName("Grep job").setMaster(master).set("spark.hadoop.validateOutputSpecs", "false");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> file = sc.textFile(inFile);
		final Map<String, Accumulator<Integer>> accums = new HashMap<String, Accumulator<Integer>>();
		for(int p = 0; p < patterns.length; p++) {
			final String pattern = patterns[p];
			accums.put("filterMatchCount-" + pattern, sc.accumulator(0));
			accums.put("filterRecordCount-" + pattern, sc.accumulator(0));
			JavaRDD<String> res = file.filter(new Function<String, Boolean>() {
				private static final long serialVersionUID = 1L;
				Pattern p = Pattern.compile(pattern);

				@Override
				public Boolean call(String value) throws Exception {
					accums.get("filterRecordCount-" + pattern).add(1);
					if (value == null || value.length() == 0) {
						return false;
					}
					final Matcher m = p.matcher(value);
					if (m.find()) {
						accums.get("filterMatchCount-" + pattern).add(1);
						return true;
					}
					return false;
				}
			});
			res.saveAsTextFile(outFile+"_"+pattern);
		}
		
		long duration = System.nanoTime() - start;
		for(String pattern : patterns) {
			System.err.println("filterMatchCount-" + pattern + ": " + accums.get("filterMatchCount-" + pattern).value());
			System.err.println("filterRecordCount-" + pattern + ": " + accums.get("filterRecordCount-" + pattern).value());
		}
		System.err.println("Runtime: " + (duration / 1000000) + "ms");
	}
}
