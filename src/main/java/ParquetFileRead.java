import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;

import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;

public class ParquetFileRead {

    public static void main(String[] args) {
        readParquetFile();
    }

    private static void readParquetFile() {
        ParquetReader<GenericData.Record> reader = null;

        Path path = new Path("EmpRecord.parquet");
        Configuration conf = new Configuration();
        conf.setBoolean(READ_INT96_AS_FIXED, true);
        try {
            reader = AvroParquetReader
                    .<GenericData.Record>builder(HadoopInputFile.fromPath(path, conf))
                    .build();
            GenericData.Record record;
            while ((record = reader.read()) != null) {
                System.out.println(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
} 