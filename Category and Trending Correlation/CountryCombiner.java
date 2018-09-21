package part1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class CountryCombiner extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();

	public void reduce(Text key, Iterable<Text> values,
			Context context
	) throws IOException, InterruptedException {

		// create a map to remember the category frequency
		// keyed on category id
		Map<String, Integer> country = new HashMap<String,Integer>();

		for (Text text: values){
			String countryId = text.toString();
						country.put(countryId, 1);
		}

		StringBuffer strBuf = new StringBuffer();
		for (String countryId: country.keySet()){
			strBuf.append(countryId+";");
		}
		result.set(strBuf.toString());
		context.write(key, result);
	}
}
