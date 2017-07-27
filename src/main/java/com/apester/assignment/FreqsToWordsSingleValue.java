package com.apester.assignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.KvSwap;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * This Transform gets as input a PCollection that holds key-value elements of words and their frequencies.
 * It returns a PCollection with a single key-value, the key is meaningless and the value holds an iterable
 * of key-value elements where the key is a frequency and the value is an iterable of words.
 * @author Administrator
 *
 */
public class FreqsToWordsSingleValue extends PTransform<PCollection<KV<String, Long>>,
PCollection<KV<String, Iterable<KV<Long, Iterable<String>>>>>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9196707135825249876L;
	private static final Logger LOG = LoggerFactory.getLogger(FreqsToWordsSingleValue.class);

	@Override
	public PCollection<KV<String, Iterable<KV<Long, Iterable<String>>>>> 
	apply(PCollection<KV<String, Long>> wordsToFrequencies) {

		LOG.info("Swapping words to frequencies -> frequencies to words.");
		PCollection<KV<Long, String>> countsToWords = wordsToFrequencies.apply(KvSwap.<String, Long>create());
		
		LOG.info("Grouping by frequencies.");
		PCollection<KV<Long, Iterable<String>>> groupedCountsToWords = countsToWords.apply(GroupByKey.<Long, String>create());
		
		LOG.info("Placing all frequency-words KVs in a value of a KV with the same meaningless key.");
		PCollection <KV<String, KV<Long, Iterable<String>>>> CountsSingleKey = groupedCountsToWords.apply(ParDo.of(new CombineToSingleList()));

		LOG.info("Group by the meaningless key to get a collection with single KV whose value holds all the frequency-words KVs");
		return CountsSingleKey.apply(GroupByKey.<String, KV<Long, Iterable<String>>>create());
	}
}
