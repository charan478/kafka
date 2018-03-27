package org.optum.canonnical;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;

import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;

public class MetaDataReader {

	private SourceBean sb;
	private String source_type;
	private String SourceLocation;
	private String SourceEntity;

	private String kafkaproperties;

	public MetaDataReader(String source_type, String sourceLocation, String sourceEntity, String kafkaproperties) {
		super();
		this.source_type = source_type;
		this.SourceLocation = sourceLocation;
		this.SourceEntity = sourceEntity;
		this.kafkaproperties = kafkaproperties;
	}

	public SourceBean extractSource(List<SourceBean> sources, String sourceEntity) {
		for (SourceBean check : sources) {
			if (sourceEntity.equals(check.getSourceEntity()))
				return check;
		}
		return null;

	}

	public Properties ReadSourceConfig() {

		Properties source_prop = new Properties();
		switch (this.source_type) {
		case "csv":
			try {
				CSVReader input = new CSVReader(new FileReader(this.SourceLocation));
				HeaderColumnNameMappingStrategy<SourceBean> beanStrategy = new HeaderColumnNameMappingStrategy<SourceBean>();
				beanStrategy.setType(SourceBean.class);

				CsvToBean<SourceBean> csvToBean = new CsvToBean<SourceBean>();
				List<SourceBean> sources = csvToBean.parse(beanStrategy, input);
				SourceBean current_source = this.extractSource(sources, this.SourceEntity);

				HashSet<String> mandcolset = new HashSet<>();
				for (String s : current_source.getCann_cols().split(current_source.getDelimiter()))
					mandcolset.add(s);

				HashSet<String> colset = new HashSet<>();
				for (String s : current_source.getCols().split(current_source.getCols()))
					colset.add(s);
				source_prop.put("mandcols", mandcolset);
				source_prop.put("cols", colset);
				source_prop.setProperty("client_id", sb.getClien_id());
				// add any other required params

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return source_prop;

	}

	private Properties updatekafkaSerdes(Properties prop)
	{
		Enumeration<Object> a = prop.keys();
		while(a.hasMoreElements())
		{
			String key=a.nextElement().toString();
			if(key.contains("SERDE"))
			{
				switch(prop.getProperty(key))
				{
				case "string":
					prop.remove(key);
					prop.setProperty(key, Serdes.String().getClass().getName());
				}
			}
		}
		
		return prop;
	}
	public Properties readKafkaProperties() {
		InputStream input = null;
		Properties kafka_prop = new Properties();
		try {
			input = new FileInputStream(this.kafkaproperties);
			kafka_prop.load(input);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				input.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return kafka_prop;

	}

	public static void main(String[] args) {

	}

}
