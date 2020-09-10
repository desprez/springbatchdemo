package com.springbatchdemo.exportjob;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.springbatchdemo.BatchTestConfiguration;
import com.springbatchdemo.common.FullReportListener;

@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = { BatchTestConfiguration.class, ExportJobConfig.class, FullReportListener.class })
public class ExportJobTest {

	@Autowired
	private JobLauncherTestUtils testUtils;

	@Autowired
	@Qualifier("exportJob")
	private Job exportJob;

	@Test
	public void importJob() throws Exception {
		// Given
		testUtils.setJob(exportJob);
		final JobParameters jobParameters = new JobParametersBuilder(testUtils.getUniqueJobParameters())
				.addString("output-file", "target/output/outputfile.csv").toJobParameters();
		// When
		final JobExecution jobExec = testUtils.launchJob(jobParameters);
		// Then
		assertThat(jobExec.getStatus(), equalTo(BatchStatus.COMPLETED));
	}

}