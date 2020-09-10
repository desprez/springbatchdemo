package com.springbatchdemo;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = { BatchTestConfiguration.class, ImportJobConfig.class, FullReportListener.class })
public class ImportJobTest {

	@Autowired
	private JobLauncherTestUtils testUtils;

	@Test
	public void importJob() throws Exception {
		// Given
		final JobParameters jobParameters = new JobParametersBuilder(testUtils.getUniqueJobParameters())
				.addString("input-file", "src/main/resources/sample-data.csv").toJobParameters();
		// When
		final JobExecution jobExec = testUtils.launchJob(jobParameters);
		// Then
		assertThat(jobExec.getStatus(), equalTo(BatchStatus.COMPLETED));
	}

}
