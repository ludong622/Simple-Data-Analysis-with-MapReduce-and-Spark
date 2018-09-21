package part1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;


public class CountryReducer extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();
	public void reduce(Text key, Iterable<Text> values,
			Context context
	) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String cty1 = conf.get("country1");
		String cty2 = conf.get("country2");
		// create a map to remember the category frequency
		// keyed on category
		Map<String, Integer> cty1Frequency = new HashMap<String,Integer>();
		// create a map to remember the video frequency
		// keyed on video id
		Map<String, Integer> cty2Frequency = new HashMap<String,Integer>();


		for (Text text: values){
			String[] value = text.toString().split(";");
			for (String s : value){
				if (s.split("@").length ==2 ){
			    String countryId = s.split("@")[0];
					String videoId = s.split("@")[1];
			    if (countryId.equals(cty1)){
						cty1Frequency.put(videoId, 1);
					}else{
						cty2Frequency.put(videoId, 1);
					}
				}
			}
		}

		int sum = 0;

		for (String countryId: cty1Frequency.keySet()){
			if (cty2Frequency.containsKey(countryId))
			{sum++;};
		}


		if (cty1Frequency.size() == 0){
			return;
		}
		else{
			StringBuffer strBuf = new StringBuffer();

			strBuf.append("total: "+cty1Frequency.size()+"; "+String.format("%.1f",(double)sum/(double)cty1Frequency.size()*100)+"% in "+cty2);

			result.set(strBuf.toString());
			context.write(key, result);
		}

	}
}
