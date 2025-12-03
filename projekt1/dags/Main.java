package com.example.bigdata;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;

//javac -cp $(hadoop classpath) Main.java -d build
//jar -cvf main.jar -C build/ .

public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Main(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job jobInstance = Job.getInstance(getConf(), "ProductSalesAggregation");
        jobInstance.setJarByClass(this.getClass());
        
        FileInputFormat.addInputPath(jobInstance, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobInstance, new Path(args[1]));

        jobInstance.setMapperClass(SalesDataMapper.class);
        jobInstance.setCombinerClass(IntermediateAggregator.class);
        jobInstance.setReducerClass(GlobalAggregator.class);
        
        jobInstance.setMapOutputKeyClass(Text.class);
        jobInstance.setMapOutputValueClass(Text.class);

        jobInstance.setOutputKeyClass(Text.class);
        jobInstance.setOutputValueClass(Text.class);
        
        return jobInstance.waitForCompletion(true) ? 0 : 1;
    }

    public static class GlobalAggregator extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text compositeKey, Iterable<Text> aggregatedValues, Context ctx)
                throws IOException, InterruptedException {

            int accumulatedCount = 0;
            int accumulatedQuantity = 0;
            double accumulatedWeightedPrice = 0.0;

            for (Text val : aggregatedValues) {
                try {
                    String[] elements = val.toString().split(",");
                    
                    if (elements.length != 3) continue;

                    int batchCount = Integer.parseInt(elements[0]);
                    int batchQty = Integer.parseInt(elements[1]);
                    double batchWeighted = Double.parseDouble(elements[2]);

                    accumulatedCount += batchCount;
                    accumulatedQuantity += batchQty;
                    accumulatedWeightedPrice += batchWeighted;
                    
                } catch (NumberFormatException nfe) {
                    continue;
                }
            }
            
            double calculatedAverage = accumulatedQuantity == 0 ? 0.0 : accumulatedWeightedPrice / accumulatedQuantity;
            
            ctx.write(compositeKey, new Text(accumulatedQuantity + "\t" + calculatedAverage));
        }
    }

    public static class IntermediateAggregator extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text compositeKey, Iterable<Text> mapperOutputs, Context ctx)
                throws IOException, InterruptedException {

            int localCount = 0;
            int localQty = 0;
            double localWeighted = 0.0;

            for (Text val : mapperOutputs) {
                try {
                    String[] tokens = val.toString().split(",");
                    
                    if (tokens.length != 3) continue;

                    int cnt = Integer.parseInt(tokens[0]);
                    int qty = Integer.parseInt(tokens[1]);
                    double weighted = Double.parseDouble(tokens[2]);

                    localCount += cnt;
                    localQty += qty;
                    localWeighted += weighted;
                    
                } catch (NumberFormatException nfe) {
                    continue;
                }
            }
            
            ctx.write(compositeKey, new Text(localCount + "," + localQty + "," + localWeighted));
        }
    }

    public static class SalesDataMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text keyOutput = new Text();
        private Text valueOutput = new Text();

        @Override
        public void map(LongWritable position, Text inputLine, Context ctx) {
            try {
                String rawLine = inputLine.toString();
                
                if (rawLine.startsWith("transaction_id")) return;

                String[] columns = rawLine.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                
                if (columns.length != 6) return;
                
                String itemId = columns[1];
                String qtyString = columns[2];
                String priceString = columns[3];
                String payType = columns[4];

                int parsedQty = Integer.parseInt(qtyString);
                double parsedPrice = Double.parseDouble(priceString);

                keyOutput.set(itemId + "\t" + payType);
                
                double weightedAmount = parsedQty * parsedPrice;
                valueOutput.set("1," + parsedQty + "," + weightedAmount);

                ctx.write(keyOutput, valueOutput);

            } catch (Exception ex) {
                System.err.println("Error processing: " + inputLine + " - " + ex.getMessage());
            }
        }
    }
}
