package com.springbatchdemo;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

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

	@Bean
	public Job importJob() {
		return jobBuilderFactory.get("import-job") //
				.incrementer(new RunIdIncrementer()) //
				.start(importStep()) //
				.listener(listener)
				.build();
	}

	@Bean
	public Step importStep() {
		return stepBuilderFactory.get("import-step") //
				.<BookDto, BookDto>chunk(5) //
				.reader(reader()) //
				.processor(processor()) //
				.writer(writer()) //
				.faultTolerant() //
				.skipPolicy(new CustomSkipPolicy()) //
				.skipLimit(2) //
				.build();
	}

	private ItemProcessor<BookDto, BookDto> processor() {
		return new ItemProcessor<BookDto, BookDto>() {

			public BookDto process(final BookDto item) throws Exception {
				LOGGER.info(item.toString());
				return item;
			}
		};
	}

	@Bean
	public FlatFileItemReader<BookDto> reader() {
		final FlatFileItemReader<BookDto> reader = new FlatFileItemReader<BookDto>();
		final DefaultLineMapper<BookDto> lineMapper = new DefaultLineMapper<BookDto>();

		final DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setDelimiter(";");
		tokenizer.setNames(new String[] { "title", "author", "isbn", "publisher", "publishedOn" });
		lineMapper.setLineTokenizer(tokenizer);

		final BeanWrapperFieldSetMapper<BookDto> fieldSetMapper = new BeanWrapperFieldSetMapper<BookDto>();
		fieldSetMapper.setTargetType(BookDto.class);
		lineMapper.setFieldSetMapper(fieldSetMapper);

		reader.setResource(new FileSystemResource("src/main/resources/sample-data.csv"));
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
