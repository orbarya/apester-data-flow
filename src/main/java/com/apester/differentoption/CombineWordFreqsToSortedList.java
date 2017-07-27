package com.apester.differentoption;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.TreeMap;

import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.values.KV;

public class CombineWordFreqsToSortedList extends CombineFn<KV<String, Long>, CombineWordFreqsToSortedList.Accum, ArrayList<KV<String, ArrayList<String>>>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3217295586945185731L;

	public static class Accum implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1628379476909558674L;
		TreeMap <String, KV<String, ArrayList<String>>> sortedFrequencies = new TreeMap<String, KV<String, ArrayList<String>>>();
		
	}
	
/*	public static class AccumCoder extends CustomCoder<Accum> {
	    private final Coder<TreeMap <Long, KV<Long, ArrayList<String>>>> treeMapCoder;

		AccumCoder (Coder<KV<String, Long>> elementCoder) {
			treeMapCoder = MapCoder.of(new VarLongCoder(), elementCoder);
		}
	    @Override
		public void encode(Accum value, OutputStream outStream,
				com.google.cloud.dataflow.sdk.coders.Coder.Context context) throws CoderException, IOException {
			
		}

		@Override
		public Accum decode(InputStream inStream, com.google.cloud.dataflow.sdk.coders.Coder.Context context)
				throws CoderException, IOException {
		}
		
	}

	@Override
	public Coder<Accum> getAccumulatorCoder(CoderRegistry registry, Coder<KV<String, Long>> inputCoder)
			throws CannotProvideCoderException {
		// TODO Auto-generated method stub
		return new AccumCoder (inputCoder);
	}*/
	
	@Override
	public Accum createAccumulator() {
		return new Accum();
	}

	@Override
	public Accum addInput(Accum accum, KV<String, Long> input) {
		Long freq = input.getValue();
		String word = input.getKey();
		if (!accum.sortedFrequencies.containsKey(String.valueOf(freq))) {
			accum.sortedFrequencies.put(String.valueOf(freq), KV.of(String.valueOf(freq), new ArrayList<String>()));
		}
		KV<String, ArrayList<String>> freqKV = accum.sortedFrequencies.get(String.valueOf(freq));
		freqKV.getValue().add(word);
		return accum;
	}

	@Override
	public Accum mergeAccumulators(Iterable<Accum> accumulators) {
		Accum result = createAccumulator();
		for (Accum accum : accumulators) {
			result.sortedFrequencies = mergeTreeMaps(result.sortedFrequencies, accum.sortedFrequencies);
		}
		return result;
	}

	@Override
	public ArrayList<KV<String, ArrayList<String>>> extractOutput(Accum accumulator) {
		ArrayList<KV<String, ArrayList<String>>> result = new ArrayList<KV<String, ArrayList<String>>>();
		for (KV<String, ArrayList<String>> kv : accumulator.sortedFrequencies.values()) {
			result.add(kv);
		}
		return result;
	}
	
	private TreeMap<String, KV<String, ArrayList<String>>> mergeTreeMaps (TreeMap<String, KV<String, ArrayList<String>>> tm1,
			TreeMap<String, KV<String, ArrayList<String>>> tm2) {
		
		TreeMap<String, KV<String, ArrayList<String>>> tm3 = new TreeMap<>(tm1);
		for (String k : tm3.keySet()) {
		    if (!tm2.containsKey(k)) {
		    	tm3.put(k, KV.of(k, new ArrayList<String>()));
		    }
	        tm3.get(k).getValue().addAll(tm2.get(k).getValue());
		} 
		return tm3;
	}
}
