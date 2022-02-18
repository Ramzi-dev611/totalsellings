package tn.insat.tp1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class ShopPriceMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private FloatWritable price = new FloatWritable();
    private Text magasin = new Text();

    public void map(Object key, Text text, Mapper.Context context)
        throws IOException, InterruptedException {
        System.out.println(text.toString());
        String[] records = text.toString().split("\n");
        for (String record: records){
            System.out.println(record);
            String[] values = record.split("\t");
            this.magasin.set(values[2]);
            try {
                this.price.set(new Float(values[values.length - 2]));
            } catch (Exception e){
                String toBeSplitMore = values[values.length - 2];
                String[] result = toBeSplitMore.split(" ");
                this.price.set(new Float(result[result.length-1]));
                System.out.println(this.price.get());
            } finally {
                context.write(magasin, price);
            }

        }
    }
}
