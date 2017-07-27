package com.apester.differentoption;

import java.util.ArrayList;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;

public class WordFreqsAsSingleValueFormatter extends DoFn<ArrayList<KV<String, ArrayList<String>>>, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5945105702561340914L;

	@Override
	public void processElement(ProcessContext context)
			throws Exception {
		String result = "";
		ArrayList<KV<String, ArrayList<String>>> sortedFreqsToWords = context.element();
		
		for (KV<String, ArrayList<String>> kv : sortedFreqsToWords) {
			
			for (String word : kv.getValue()) {
				result += word + ": " + kv.getKey() + "\r\n";
			}
		}
		context.output(result);
	}
}
