package com.springbatchdemo.importjob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class MyStepListener implements StepExecutionListener {

	Logger logger = LoggerFactory.getLogger(MyStepListener.class);

	public void beforeStep(final StepExecution stepExecution) {
		logger.info("Before Step");
	}

	public ExitStatus afterStep(final StepExecution stepExecution) {
		// TODO Auto-generated method stub
		return null;
	}

}
