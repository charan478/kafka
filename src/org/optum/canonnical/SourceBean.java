package org.optum.canonnical;

public class SourceBean {
	private String SourceEntity;
	private String cann_cols;
	private String cols;
	private String clien_id;
	private String delimiter;
	
	public String getClien_id() {
		return clien_id;
	}

	public void setClien_id(String clien_id) {
		this.clien_id = clien_id;
	}

	public SourceBean(String source_type, String sourceLocation, String sourceEntity) {
		super();
	this.SourceEntity = sourceEntity;
	}
	
	public String getSourceEntity() {
		return SourceEntity;
	}
	public void setSourceEntity(String sourceEntity) {
		SourceEntity = sourceEntity;
	}

	public String getCann_cols() {
		return cann_cols;
	}

	public void setCann_cols(String cann_cols) {
		this.cann_cols = cann_cols;
	}

	public String getCols() {
		return cols;
	}

	public void setCols(String cols) {
		this.cols = cols;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}
}
