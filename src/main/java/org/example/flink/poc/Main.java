package org.example.flink.poc;

import com.google.auth.oauth2.GoogleCredentials;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/Users/navi/Desktop/Digipass");
        System.out.println("Hello world!");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        // Step 2 -> Load credentials from the downloaded JSON file
        GoogleCredentials googleCredentials = GoogleCredentials.fromStream(Main.class.getClassLoader().getResourceAsStream("wired-record-443707-u8-618011b336b0.json"));
        DeserializationSchema<JsonNode> deserializer = new DeserializationSchema<JsonNode>() {
            @Override
            public JsonNode deserialize(byte[] message) throws IOException {
                // Deserialize the message to a JsonNode (or just a String if your message is plain text)
                return new ObjectMapper().readTree(message);
            }

            @Override
            public boolean isEndOfStream(JsonNode nextElement) {
                return false;
            }

            @Override
            public TypeInformation<JsonNode> getProducedType() {
                return TypeInformation.of(JsonNode.class);
            }
        };

        // Step 4 -> Set up the Pub/Sub source to read messages from the topic
        PubSubSource<JsonNode> pubSubSource = PubSubSource.newBuilder()
                .withDeserializationSchema(deserializer)
                .withProjectName("wired-record-443707-u8")  // Replace with your Google Cloud project ID
                .withSubscriptionName("iceberg-pipeline-topic-sub")  // Replace with your subscription name
                .withCredentials(googleCredentials)  // Use the loaded credentials
                .build();

        // Step 5 -> Add the Pub/Sub source to the Flink job
        DataStream<JsonNode> pubSubMessages = env.addSource(pubSubSource);

        DataStream<RowData> tableData = pubSubMessages.map(msg -> {
            System.out.println("Received Msg: " + msg.toString());
            GenericRowData row = new GenericRowData(1);
            row.setField(0, StringData.fromString(msg.toString()));
            return row;
        });

        Schema schema = new Schema(
                Types.NestedField.optional(0, "status", Types.StringType.get())
        );
        String db = "db";
        String warehouseLocation = "gs://catalog_bucket_test/warehouse";
        String tableName = "flink_sink_table";
        Configuration hadoopConf = new Configuration();

        HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehouseLocation);
        // Step -4 Define the table identifier
        TableIdentifier tableId = TableIdentifier.of(db, tableName);
        if(!catalog.tableExists(tableId)){
            catalog.createTable(tableId, schema);
        }
        TableLoader tableLoader = TableLoader.fromHadoopTable(warehouseLocation + "/" + db + "/" + tableName, hadoopConf);
        //Step 5-  Write the data to Iceberg
        FlinkSink.forRowData(tableData)
                .tableLoader(tableLoader)
                .table(catalog.loadTable(tableId))
                .overwrite(true).append();


        // Step 7 -> Execute the Flink job (this will start consuming messages from Pub/Sub)
        env.execute("Flink Pub/Sub Message Reader");
    }
}