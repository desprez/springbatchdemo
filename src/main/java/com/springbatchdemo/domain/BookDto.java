package com.springbatchdemo.domain;

public class BookDto {

	private String title;

	private String author;

	private String isbn;

	private String publisher;

	private Integer publishedOn;

	// Constructors, Getters and Setters
	public BookDto() {
		super();
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(final String title) {
		this.title = title;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(final String author) {
		this.author = author;
	}

	public String getIsbn() {
		return isbn;
	}

	public void setIsbn(final String isbn) {
		this.isbn = isbn;
	}

	public String getPublisher() {
		return publisher;
	}

	public void setPublisher(final String publisher) {
		this.publisher = publisher;
	}

	public Integer getPublishedOn() {
		return publishedOn;
	}

	public void setPublishedOn(final Integer publishedOn) {
		this.publishedOn = publishedOn;
	}

	@Override
	public String toString() {
		return "BookDto [title=" + title + ", author=" + author + ", isbn=" + isbn + ", publisher=" + publisher
				+ ", publishedOn=" + publishedOn + "]";
	}

}
