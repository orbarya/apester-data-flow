/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.apester.assignment;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.ParDo;


public class CountWordsPipeline {

  public static void main(String[] args) {

	PipelineOptionsFactory.register(WordCountIOFilesOptions.class);
	
	WordCountIOFilesOptions options = PipelineOptionsFactory.fromArgs(args)            
            .as(WordCountIOFilesOptions.class);
	options.setProject("1");
	options.setStagingLocation("./staging/");
	options.setRunner(DirectPipelineRunner.class);

    Pipeline p = Pipeline.create(options);

	
	p.apply(TextIO.Read.from(options.getInputFilePath()))
	
	.apply(new CountWords())
	
	//PCollection<ArrayList<KV<String, ArrayList<String>>>> sortedFreqsToWordsAsSingleValue = wordsToFrequencies.apply(Combine.globally(new CombineWordFreqsToSortedList()));
	
	//PCollection<String>formattedWordsFreqs = sortedFreqsToWordsAsSingleValue.apply(ParDo.named("formatFreqsToWords").of(new WordFreqsAsSingleValueFormatter()));

	
	.apply(new FreqsToWordsSingleValue())
	
	.apply(ParDo.named("formatFreqsToWords").of(new WordsFreqsFormatterAsSingleValue()))
	
	.apply(TextIO.Write.to(options.getOutputFilePath()));
    
    p.run();

  }
}
