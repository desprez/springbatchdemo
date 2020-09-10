package com.springbatchdemo.importjob;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import com.springbatchdemo.common.FullReportListener;
import com.springbatchdemo.domain.BookDto;

@Configuration
public class ImportJobConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(ImportJobConfig.class);

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private DataSource dataSource;

	@Autowired
	private FullReportListener listener;

	@Autowired
	private DeleteTasklet deleteTasklet;

	@Bean
	public Job importJob() {
		return jobBuilderFactory.get("import-job") //
				.incrementer(new RunIdIncrementer()) //
				.start(deleteStep()) //
				.next(importStep()) //
				.listener(listener) //
				.build();
	}

	/**
	 * Delete Step for deleting all Book records.
	 *
	 * @return the Step
	 */
	@Bean
	public Step deleteStep() {
		return stepBuilderFactory.get("delete-step") //
				.tasklet(deleteTasklet) //
				.build();
	}

	@Bean
	public Step importStep() {
		return stepBuilderFactory.get("import-step") //
				.<BookDto, BookDto>chunk(5) //
				.reader(reader(null)) //
				.processor(processor()) //
				.writer(writer()) //
				.faultTolerant() //
				.skipPolicy(new CustomSkipPolicy()) //
				.skipLimit(2) //
				.build();
	}

	/**
	 * Fake processor that only logs
	 *
	 * @return an item processor
	 */
	private ItemProcessor<BookDto, BookDto> processor() {
		return new ItemProcessor<BookDto, BookDto>() {

			public BookDto process(final BookDto item) throws Exception {
				LOGGER.info(item.toString());
				return item;
			}
		};
	}

	@StepScope // Mandatory for using jobParameters
	@Bean
	public FlatFileItemReader<BookDto> reader(@Value("#{jobParameters['input-file']}") final String inputFile) {
		final FlatFileItemReader<BookDto> reader = new FlatFileItemReader<BookDto>();
		final DefaultLineMapper<BookDto> lineMapper = new DefaultLineMapper<BookDto>();

		final DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setDelimiter(";");
		tokenizer.setNames(new String[] { "title", "author", "isbn", "publisher", "publishedOn" });
		lineMapper.setLineTokenizer(tokenizer);

		final BeanWrapperFieldSetMapper<BookDto> fieldSetMapper = new BeanWrapperFieldSetMapper<BookDto>();
		fieldSetMapper.setTargetType(BookDto.class);
		lineMapper.setFieldSetMapper(fieldSetMapper);

		reader.setResource(new FileSystemResource(inputFile));
		reader.setLineMapper(lineMapper);
		reader.setLinesToSkip(1);
		return reader;
	}

	@Bean
	public JdbcBatchItemWriter<BookDto> writer() {
		final JdbcBatchItemWriter<BookDto> writer = new JdbcBatchItemWriter<BookDto>();
		writer.setDataSource(dataSource);
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<BookDto>());
		writer.setSql(
				"INSERT INTO book(title, author, isbn, publisher, year) VALUES (:title, :author, :isbn, :publisher, :publishedOn )");
		return writer;
	}
}
