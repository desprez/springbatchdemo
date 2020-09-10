package com.springbatchdemo.importjob;

import javax.sql.DataSource;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class DeleteTasklet implements Tasklet {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private DataSource dataSource;

	public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext) throws Exception {

		jdbcTemplate.setDataSource(dataSource);
		jdbcTemplate.batchUpdate("DELETE FROM Book");

		return RepeatStatus.FINISHED;
	}

}
