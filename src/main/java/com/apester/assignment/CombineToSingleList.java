package com.apester.assignment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;

public class CombineToSingleList extends DoFn<KV<Long, Iterable<String>>, KV<String, KV<Long, Iterable<String>>>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4687861208050010208L;
	private static final Logger LOG = LoggerFactory.getLogger(CombineToSingleList.class);
    
	private final Aggregator<Long, Long> frequenciesCombinedToSingleValueSuccessfuly =
            createAggregator("frequenciesCombinedToSingleValueSuccessfuly", new Sum.SumLongFn());

    private final Aggregator<Long, Long> frequenciesCombinedToSingleValueFailures =
            createAggregator("frequenciesCombinedToSingleValueFailures", new Sum.SumLongFn());

	@Override
	public void processElement(ProcessContext context)
			throws Exception {
		try {
			KV<String, KV<Long, Iterable<String>>> kv = KV.of("key", context.element());
			context.output(kv);
			frequenciesCombinedToSingleValueSuccessfuly.addValue(1L);
		} catch (Exception e) {
			LOG.error(String.format("Unable to combine element with frequency %s"),context.element().getKey(), e);
			frequenciesCombinedToSingleValueFailures.addValue(1L);
		}
	}
}
