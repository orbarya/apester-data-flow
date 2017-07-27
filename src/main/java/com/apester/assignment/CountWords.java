package com.apester.assignment;

import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1502550637597150292L;

	@Override
	public PCollection<KV<String, Long>> apply(PCollection<String> lines) {
		
		return 
		lines.apply(ParDo.named("tokenizeWords").of(new WordsTokenizer()))
		.apply(Count.<String>perElement());
	}
}
