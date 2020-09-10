package com.springbatchdemo.exportjob;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;

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
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;

import com.springbatchdemo.domain.BookDto;
import com.springbatchdemo.importjob.ImportJobConfig;

@Configuration
public class ExportJobConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(ImportJobConfig.class);

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Bean
	public Step exportStep(final FlatFileItemWriter<BookDto> exportWriter) {
		return stepBuilderFactory.get("export-step").<BookDto, BookDto>chunk(10) //
				.reader(exportReader()) //
				.processor(exportProcessor()) //
				.writer(exportWriter) //
				.build();
	}

	@Bean(name = "exportJob")
	public Job exportBookJob(final Step exportStep) {
		return jobBuilderFactory.get("export-job") //
				.incrementer(new RunIdIncrementer()) //
				.flow(exportStep) //
				.end() //
				.build();
	}

	/**
	 * ItemReader is an abstract representation of how data is provided as input to
	 * a Step. When the inputs are exhausted, the ItemReader returns null.
	 */
	@Bean
	public JdbcCursorItemReader<BookDto> exportReader() {
		final JdbcCursorItemReader<BookDto> reader = new JdbcCursorItemReader<BookDto>();
		reader.setDataSource(dataSource);
		reader.setSql("SELECT title, author, isbn, publisher, year FROM Book");
		reader.setRowMapper(new BookRowMapper());

		return reader;
	}

	/**
	 * RowMapper used to map resultset to BookDto
	 */
	public class BookRowMapper implements RowMapper<BookDto> {

		public BookDto mapRow(final ResultSet rs, final int rowNum) throws SQLException {
			final BookDto book = new BookDto();
			book.setTitle(rs.getString("title"));
			book.setAuthor(rs.getString("author"));
			book.setIsbn(rs.getString("isbn"));
			book.setPublisher(rs.getString("publisher"));
			book.setPublishedOn(rs.getInt("year"));
			return book;
		}
	}

	/**
	 * ItemProcessor represents the business processing of an item. The data read by
	 * ItemReader can be passed on to ItemProcessor. In this unit, the data is
	 * transformed and sent for writing. If, while processing the item, it becomes
	 * invalid for further processing, you can return null. The nulls are not
	 * written by ItemWriter.
	 */
	@Bean
	public ItemProcessor<BookDto, BookDto> exportProcessor() {
		return new ItemProcessor<BookDto, BookDto>() {

			public BookDto process(final BookDto book) throws Exception {
				LOGGER.info("Processing {}", book);
				return book;
			}
		};
	}

	/**
	 * ItemWriter is the output of a Step. The writer writes one batch or chunk of
	 * items at a time to the target system. ItemWriter has no knowledge of the
	 * input it will receive next, only the item that was passed in its current
	 * invocation.
	 */
	@StepScope // Mandatory for using jobParameters
	@Bean
	public FlatFileItemWriter<BookDto> exportWriter(@Value("#{jobParameters['output-file']}") final String outputFile) {
		final FlatFileItemWriter<BookDto> writer = new FlatFileItemWriter<BookDto>();
		writer.setResource(new FileSystemResource(outputFile));
		final DelimitedLineAggregator<BookDto> lineAggregator = new DelimitedLineAggregator<BookDto>();
		final BeanWrapperFieldExtractor<BookDto> fieldExtractor = new BeanWrapperFieldExtractor<BookDto>();
		fieldExtractor.setNames(new String[] { "title", "author", "isbn", "publisher", "publishedOn" });
		lineAggregator.setFieldExtractor(fieldExtractor);
		lineAggregator.setDelimiter(";");
		writer.setLineAggregator(lineAggregator);
		writer.setHeaderCallback(new FlatFileHeaderCallback() {
			public void writeHeader(final Writer writer) throws IOException {
				writer.write("title;author;isbn;publisher;publishedOn");
			}
		});
		return writer;
	}
}
