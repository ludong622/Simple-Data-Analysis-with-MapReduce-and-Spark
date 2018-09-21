package part1;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;


public class CountryMapper extends Mapper<Object, Text, Text, Text> {

    private Text word = new Text(), category = new Text(), video = new Text();

	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();
      String str1 = conf.get("country1");
      String str2 = conf.get("country2");

			if (value.toString().substring(value.toString().length()-2,value.toString().length()).equals(str1) || value.toString().substring(value.toString().length()-2,value.toString().length()).equals(str2))
		{
			String[] dataArray = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); //split the data into array
		if (dataArray.length < 18){  // record with incomplete data
			return; // don't emit anything
	       	}
		String countryString = dataArray[17];
	       	if (!countryString.equals(str1) && !countryString.equals(str2)){
		    return;
	       	}
		String videoidString = dataArray[0];
		String categoryString = dataArray[5];
	        word.set(categoryString);
		category.set(countryString+"@"+videoidString);
	}else{return;}

	        context.write(word, category);

	}
}
