import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;

public class ParquetFileWrite {

    public static void main(String[] args) {
        // First thing - parse the schema as it will be used
        Schema schema = parseSchema();
        List<GenericData.Record> recordList = getRecords(schema);
        writeToParquet(recordList, schema);
    }

    private static Schema parseSchema() {
        Schema.Parser parser = new    Schema.Parser();
        Schema schema = null;
        try {
            // pass path to schema
            schema = parser.parse(ClassLoader.getSystemResourceAsStream(
                    "schema.json"));

        } catch (IOException e) {
            e.printStackTrace();
        }
        return schema;

    }

    private static List<GenericData.Record> getRecords(Schema schema){
        List<GenericData.Record> recordList = new ArrayList<GenericData.Record>();
        GenericData.Record record = new GenericData.Record(schema);

        List<GenericData.Record> elementList = new ArrayList();

        Schema listSchema = record.getSchema().getField("coordinates").schema().getTypes().get(1).getElementType();
        Schema elementSchema = listSchema.getField("element").schema().getTypes().get(1);
        GenericData.Record elementRecord = new GenericData.Record(elementSchema);
        GenericData.Record listRecord = new GenericData.Record(listSchema);

        elementRecord.put("n", 0);
        elementRecord.put("x", 0);
        elementRecord.put("y", 0);

        listRecord.put("element", elementRecord);

        elementList.add(listRecord);

        record.put("assetId", "e749c256-d3d3-4afd-bbd6-404e1ce1b45a");
        record.put("contentType", "SPECTRUM");
        record.put("coordinates", elementList);
        record.put("coordinates", elementList);

        recordList.add(record);
        return recordList;
    }


    private static void writeToParquet(List<GenericData.Record> recordList, Schema schema) {
        // Path to Parquet file in HDFS
        Path path = new Path("EmpRecord.parquet");
        ParquetWriter<GenericData.Record> writer = null;
        Configuration conf = new Configuration();
        // Creating ParquetWriter using builder
        try {
            writer = AvroParquetWriter.
                    <GenericData.Record>builder(path)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withSchema(schema)
                    .withConf(conf)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .build();

            for (GenericData.Record record : recordList) {
                writer.write(record);
            }

        }catch(IOException e) {
            e.printStackTrace();
        }finally {
            if(writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}