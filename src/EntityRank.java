//package hello;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.rank;
import static org.apache.spark.sql.functions.row_number;


public class EntityRank {

    public static final String[] ENTITY = { "modi",
            "rahul",
            "jaitley",
            "sonia",
            "lalu",
            "nitish",
            "farooq",
            "sushma",
            "tharoor",
            "smriti",
            "mamata",
            "karunanidhi",
            "kejriwal",
            "sidhu",
            "yogi",
            "mayawati",
            "akhilesh",
            "chandrababu",
            "chidambaram",
            "fadnavis",
            "uddhav",
            "pawar",
    };

    public static void main(String[] args) {

        //Input dir - should contain all input json files
        String inputPath="C:\\Users\\bhavna\\Downloads\\newsdata"; //Use absolute paths
        //Ouput dir - this directory will be created by spark. Delete this directory between each run
        //String outputPath="C:\\Users\\bhavna\\Downloads\\output";//Use absolute paths


        Logger.getLogger("org").setLevel(Level.OFF);//Remove Log info
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession sparkSession = SparkSession.builder()
                .appName("Entity Count")		//Name of application
                .master("local")								//Run the application on local node
                .config("spark.sql.shuffle.partitions","2")		//Number of partitions
                .getOrCreate();

        Dataset<Row> inputDataset=sparkSession.read().option("multiLine", true).json(inputPath); //Read Json files

        StructType schema = new StructType(); //Create the schema for empty dataset
        schema = schema.add("source_name", DataTypes.StringType, true, Metadata.empty())
                .add("article_id", DataTypes.StringType, true, Metadata.empty())
                .add("entity", DataTypes.StringType, true, Metadata.empty());

        Dataset<Row> ds = sparkSession.read().schema(schema).csv(sparkSession.emptyDataset(Encoders.STRING())); //Empty dataset

        //Loop through each entity and check if they contains in row ("article_body")
        for(int i=0;i<ENTITY.length;i++) {

            Dataset<Row> temp= inputDataset.filter(lower(inputDataset.col("article_body")).contains(ENTITY[i])).select("source_name","article_id").withColumn("entity",functions.lit(ENTITY[i]));
            ds=ds.union(temp); //add to existing rows

        }
        //Q 1
        ds=ds.select("entity","source_name","article_id");
        ds.show((int)ds.count(),false); //Q 1 Remove false if you want to truncate
        //Q 2
        Dataset<Row> ds1 = ds.groupBy("source_name","entity").count();
        ds1.show((int)ds1.count(),false); //Q 2 Remove false if you want to truncate
        //Q 3
        //create partitions of source name and sort by count
        WindowSpec temp = Window.partitionBy("source_name").orderBy(ds1.col("count").desc());
        //add two column rank of count and row number
        Dataset<Row> ds2 = ds1.withColumn("rank",rank().over(temp)).withColumn("num", row_number().over(temp)).filter("num <= 5");
        ds2 = ds2.select("source_name","entity","rank","count");
        ds2.show((int)ds2.count(),false); //Q 3 Remove false if you want to truncate
    }
}


