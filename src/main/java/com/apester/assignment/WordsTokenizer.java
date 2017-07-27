package com.apester.assignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;

public class WordsTokenizer extends DoFn<String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5565774866132132171L;

	private static final Logger LOG = LoggerFactory.getLogger(WordsTokenizer.class);

	private final Aggregator<Long, Long> linesTokenizedSuccessfuly =
            createAggregator("linesTokenizedSuccessfuly", new Sum.SumLongFn());

	private final Aggregator<Long, Long> linesTokenizedFailures =
            createAggregator("linesTokenizedFailures", new Sum.SumLongFn());

	@Override
	public void processElement(ProcessContext c) throws Exception {
		try {
			LOG.trace("Processing line : " + c.element());
			String[] words = c.element().split("[^a-zA-Z']+"); 
			for (String word : words) {
				if (!word.isEmpty()) {
					c.output(word);
				}
			}
			linesTokenizedSuccessfuly.addValue(1L);
		}catch (Exception e) {
			LOG.error(String.format("Failed processing line : %s", c.element()), e);
			linesTokenizedFailures.addValue(1L);
		}
	}
}
