import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.kitesdk.data.spi.JsonUtil;
import org.kitesdk.data.spi.filesystem.JSONFileReader;

import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;

public class ParquetFileWrite {

    public static void main(String[] args) {
        // First thing - parse the schema as it will be used
//        Schema schema = parseSchema();
//        List<GenericRecord> recordList = getRecords(schema);
        writeToParquet();
    }

//    private static Schema parseSchema() {
//        Schema.Parser parser = new    Schema.Parser();
//        Schema schema = null;
//        try {
//            // pass path to schema
//            schema = parser.parse(ClassLoader.getSystemResourceAsStream(
//                    "schema.json"));
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return schema;
//
//    }

    private static List<GenericRecord> getRecords(Schema schema){
        List<GenericRecord> recordList = new ArrayList<GenericRecord>();
        GenericRecord record = new GenericData.Record(schema);

        List<GenericData.Record> elementList = new ArrayList();

        Schema listSchema = record.getSchema().getField("coordinates").schema().getTypes().get(1).getElementType();
        Schema elementSchema = listSchema.getField("element").schema().getTypes().get(1);
        GenericData.Record elementRecord = new GenericData.Record(elementSchema);
        GenericData.Record listRecord = new GenericData.Record(listSchema);

        elementRecord.put("n", 0);
        elementRecord.put("x", 0);
        elementRecord.put("y", 0);

        listRecord.put("element", elementRecord);

        elementList.add(elementRecord);

        record.put("assetId", "e749c256-d3d3-4afd-bbd6-404e1ce1b45a");
        record.put("contentType", "SPECTRUM");
        record.put("coordinates", elementList);

        recordList.add(record);
        return recordList;
    }


    private static void writeToParquet() {

        Path path = new Path("EmpRecord.parquet");
        ParquetWriter<GenericRecord> writer = null;
        Configuration conf = new Configuration();
        try (FileInputStream fileInputStream = new FileInputStream(new File(ClassLoader.getSystemResource("parquet_to_json.json").getPath()))) {
            Schema jsonSchema = JsonUtil.inferSchema(fileInputStream, null, 1000);
            try (JSONFileReader<GenericRecord> reader = new JSONFileReader<>(
                    fileInputStream, jsonSchema, GenericRecord.class)) {
                reader.initialize();
                writer = AvroParquetWriter.
                        <GenericRecord>builder(HadoopOutputFile.fromPath(path, conf))
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                        .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                        .withSchema(jsonSchema)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withValidation(false)
                        .withDictionaryEncoding(false)
                        .build();

                for (GenericRecord record : reader) {
                    writer.write(record);
                }

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (writer != null) {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}