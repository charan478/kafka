package org.optum.canonnical;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Properties;

public class JsonCannonicalBUilder {

	public static void main(final String[] args) throws Exception {
		
		if(args.length!=4)
		{
			System.out.println("parameter order is source_type source_location source_entity kakfaprops");
			System.exit(0);
		}
		String source_type= args[0];
		String sourceLocation=args[1];
		String sourceEntity= args[2];
		String kafkaproperties= args[3];
		
		MetaDataReader meta= new MetaDataReader(source_type, sourceLocation, sourceEntity, kafkaproperties);
		final Properties streamsConfiguration = meta.readKafkaProperties();
		final Properties source_props=meta.ReadSourceConfig();
	
		
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "cannonical_Builder");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "cannonical_Builder-client");
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

		final Serde<String> stringSerde = Serdes.String();
		final StreamsBuilder builder = new StreamsBuilder();
		final KStream<String, String> textLines = builder.stream("charan");

		@SuppressWarnings("unchecked")
		KStream<String, String>[] branches = textLines.branch(new Predicate<String, String>() {
			@Override
			public boolean test(String key, String value) {
				JSONObject j;
				try {
					j = new JSONObject(value);
					boolean valid = true;
					for (String s : (HashSet<String>)source_props.get("mandcols"))
						if (!j.has(s))
							valid = false;
					return !valid;
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					return true;
				}

			}
		}, (key, value) -> true);

		KStream<String, String> transformed = branches[1]
				.map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
					@Override
					public KeyValue<String, String> apply(String key, String value) {
						JSONObject ret = new JSONObject();

						try {
							JSONObject j = new JSONObject(value);

							for (String s : (HashSet<String>)source_props.get("cols"))
								if (j.has(s))
									ret.put(s, j.get(s));
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						return new KeyValue<>(key, ret.toString());
					}
				});

		branches[0].to("error", Produced.with(stringSerde, stringSerde));
		transformed.to("cannonical", Produced.with(stringSerde, stringSerde));

		final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.cleanUp();
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

}