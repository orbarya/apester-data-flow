package com.apester.assignment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;

public class WordsFreqsFormatterAsSingleValue extends DoFn<KV<String, Iterable<KV<Long, Iterable<String>>>>, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6398321973234827181L;
	private static final Logger LOG = LoggerFactory.getLogger(WordsFreqsFormatterAsSingleValue.class);

	private final Aggregator<Long, Long> addedFrequenciesToCollection =
            createAggregator("addedFrequenciesToCollection", new Sum.SumLongFn());

	private final Aggregator<Long, Long> formattedFrequenciesSuccessfuly =
            createAggregator("formattedFrequenciesSuccessfuly", new Sum.SumLongFn());

	private final Aggregator<Long, Long> formattedFrequenciesFailures =
            createAggregator("formattedFrequenciesFailures", new Sum.SumLongFn());

	private final Aggregator<Long, Long> formattedWordsSuccessfuly =
            createAggregator("formattedWordsSuccessfuly", new Sum.SumLongFn());

	private final Aggregator<Long, Long> formattedWordsFailures =
            createAggregator("formattedWordsFailures", new Sum.SumLongFn());
	
	@Override
	public void processElement(ProcessContext context) throws Exception {
		KV<Long, Iterable<String>> freqWordsKV = null;
		Iterator<String> wordsIterator = null;
		LOG.info("Formatting words-freqs output.");
		Iterator<KV<Long, Iterable<String>>> freqsIterator = 
				sortIterableKVsReveresed(context.element().getValue()).iterator();

		StringBuilder result = new StringBuilder();
		while (freqsIterator.hasNext()) {
			Long freqValue = null;

			try {
				freqWordsKV = freqsIterator.next();
				freqValue = freqWordsKV.getKey();

				wordsIterator = freqWordsKV.getValue().iterator();
				LOG.trace("iterating freq %s", freqValue);
				while (wordsIterator.hasNext()) {
					String word = null;
					try {
					word = wordsIterator.next();
					LOG.trace("word %s", word);
					result.append(word + ": " + freqValue + "\r\n");
					} catch (Exception e) {
						LOG.error(String.format("Failed processing word %s", word), e);
						formattedWordsFailures.addValue(1L);
					}
					formattedWordsSuccessfuly.addValue(1L);
				}
			} catch (Exception e) {
				LOG.error(String.format("Failed processing frequency %s", freqValue));
				formattedFrequenciesFailures.addValue(1L);
			}
			formattedFrequenciesSuccessfuly.addValue(1L);
		}
		context.output(result.toString());
	}

	private List<KV<Long, Iterable<String>>> sortIterableKVsReveresed (Iterable<KV<Long, Iterable<String>>> iterable) {
		List<KV<Long, Iterable<String>>> sortedList = new ArrayList<KV<Long, Iterable<String>>>();
		LOG.info("Adding all frequency-words elements to collection.");
		for (KV<Long, Iterable<String>> kv : iterable) {
			sortedList.add(kv);
			addedFrequenciesToCollection.addValue(1L);
		}
		LOG.info("Sorting the collection according to frequency.");		
		Collections.sort(sortedList,new KV.OrderByKey<Long, Iterable<String>>());
		LOG.info("Reversing the collection.");				
		Collections.reverse(sortedList);
		return sortedList;
	}
}
