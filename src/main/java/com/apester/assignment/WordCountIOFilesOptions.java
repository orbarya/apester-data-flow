package com.apester.assignment;

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;

public interface WordCountIOFilesOptions extends DataflowPipelineOptions {

	String getInputFilePath ();
	void setInputFilePath (String inputFilePath);
	String getOutputFilePath ();
	void setOutputFilePath (String outputFilePath);
	
}
