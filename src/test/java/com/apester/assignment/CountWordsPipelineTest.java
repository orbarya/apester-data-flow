package com.apester.assignment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarLongCoder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;


public class CountWordsPipelineTest {

	// Keeps this list sorted in descending order since it is relied upon in some tests.
	List<KV<Long, Iterable<String>>> freqsToWordsKVs = (Arrays.asList(
			(KV.of(7L, (Iterable<String>)Arrays.asList(new String[]{"it"}))),
			(KV.of(5L, (Iterable<String>)Arrays.asList(new String[]{"R","F", "C"}))),	
			(KV.of(3L, (Iterable<String>)Arrays.asList(new String[]{"A", "B"}))),
			(KV.of(1L, (Iterable<String>)Arrays.asList(new String[]{"one","two","three"})))));

	@Test
	public void testWordsTokenizer() {

		List<String> inputLines = Arrays.asList(new String[]{"Whose action is no stronger than a flower?",
				"",
				" ",
				"\r\n",
				"They know what beauty is, see where it lies",
		"  Bear thine eyes straight, though thy proud heart go wide."});

		Pipeline p = TestPipeline.create();

		// Create an input PCollection.
		PCollection<String> linesCollection = p.apply(Create.of(inputLines)).setCoder(StringUtf8Coder.of());

		String[] expectedWords = {"Whose","action","is","no","stronger","than",
				"a","flower","They","know","what","beauty","is","see","where",
				"it","lies","Bear","thine","eyes","straight","though","thy",
				"proud","heart","go","wide"};

		PCollection<String> words = linesCollection.apply(ParDo.of(new WordsTokenizer()));

		DataflowAssert.that(words).containsInAnyOrder(expectedWords);

		p.run();
	}

	@Test
	public void testCombineToSingleList() {

		Pipeline p = TestPipeline.create();

		PCollection<KV<Long, Iterable<String>>> freqsToWordsKVsCollection = 
				p.apply(Create.of(freqsToWordsKVs).withCoder(KvCoder.of(VarLongCoder.of(),
						IterableCoder.of(StringUtf8Coder.of()))));

		List<KV<String, KV<Long, Iterable<String>>>> expected = new ArrayList <KV<String, KV<Long, Iterable<String>>>> ();
		for ( KV<Long, Iterable<String>> kv : freqsToWordsKVs) {
			expected.add(KV.of("key", kv));
		}

		PCollection<KV<String, KV<Long, Iterable<String>>>>  resultCollection =
				freqsToWordsKVsCollection.apply(ParDo.of(new CombineToSingleList()));

		DataflowAssert.that(resultCollection).containsInAnyOrder(expected);

		p.run();
	}

	@Test
	public void testWordsFreqsFormatterAsSingleValue () {
		List<KV<String, Iterable<KV<Long, Iterable<String>>>>> input = new ArrayList<KV<String, Iterable<KV<Long, Iterable<String>>>>> ();

		input.add(KV.of("key", (Iterable<KV<Long, Iterable<String>>>)new ArrayList
				<KV<Long, Iterable<String>>> (freqsToWordsKVs)));

		String expected = "";
		for (KV<Long, Iterable<String>> kv : freqsToWordsKVs) {
			for (String word : kv.getValue()) {
				expected += kv.getKey() + ": " + word + "\r\n";
			}
		}
		
		Pipeline p = TestPipeline.create();
		
		PCollection<KV<String, Iterable<KV<Long, Iterable<String>>>>> freqsToWordsAsSingleValue = 
				p.apply(Create.of(input).withCoder(KvCoder.of(StringUtf8Coder.of(),
						IterableCoder.of(KvCoder.of(VarLongCoder.of(), IterableCoder.of(StringUtf8Coder.of()))))));
		
		PCollection<String> result = freqsToWordsAsSingleValue.apply(ParDo.of(new WordsFreqsFormatterAsSingleValue()));
		
		DataflowAssert.that(result).containsInAnyOrder(expected);
	}

}
