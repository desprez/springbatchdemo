package com.springbatchdemo.importjob;

import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.file.FlatFileParseException;

public class CustomSkipPolicy implements SkipPolicy {

	public boolean shouldSkip(final Throwable t, final int skipCount) throws SkipLimitExceededException {

		if (t instanceof FlatFileParseException) {
			return true;
		}

		return false;
	}

}
