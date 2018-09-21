package part2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.*;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import java.io.*;


public class ViewingNumber {


	public static void main(String[] args) {

	    String inputDataPath = args[0], outputDataPath = args[1];
	    SparkConf conf = new SparkConf();

	    conf.setAppName("Viewing number Application");

	    JavaSparkContext sc = new JavaSparkContext(conf);

	    JavaRDD<String> viewingNumberData = sc.textFile(inputDataPath);


	    JavaPairRDD<String, String> extraction = viewingNumberData.mapToPair(s ->
	    	{  String[] values = s.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
		   String videoID = values[0];
		   String trendingDate = values[1];
		   String viewingNumber = values[8];
		   String country = values[17];
	    		return
	    			new Tuple2<String, String>(country+"; "+videoID,trendingDate+"@"+viewingNumber);
	    	}
	    );


	    JavaPairRDD<String, Iterable<String>> group = extraction.groupByKey();

       JavaPairRDD<String, String> timeFilter = group.mapToPair(s ->
          {
           String videoID = s._1;
           ArrayList<String> values = new ArrayList<String>();
           for(String ss : s._2){
             String[] splitString = ss.split("@");

               String dateinString = splitString[0];
               String[] str = dateinString.split("\\.");
               if (str.length == 3){
                 String temp = str[0]+str[2]+str[1];
                 String newValue = temp+"@"+splitString[1];
                 values.add(newValue);
               }

           }

          if (values.size() >= 2){
            String[] st = {"",""};
            for(String value : values){

              if (st[0].equals("")){st[0]=value;}
              else if (st[0].split("@")[0].compareTo(value.split("@")[0]) > 0){st[0]=value;}
              else if (st[1].equals("")){st[1]=value;}
              else if (st[1].split("@")[0].compareTo(value.split("@")[0]) > 0){st[1]=value;}

            }
            String percentage = String.format("%.1f",(Double.parseDouble(st[1].split("@")[1])/Double.parseDouble(st[0].split("@")[1])-1)*100);
            if (Double.parseDouble(percentage) >= 1000){
              if (videoID.split("; ").length == 2){
                return new Tuple2<String,String>(videoID+", "+percentage+"%","");
              }else{return new Tuple2<String,String>("0","0");}
            }
            else{return new Tuple2<String,String>("0","0");}
          }else{
            return new Tuple2<String,String>("0","0");
          }
        }
      ).filter(t->!t._1.equals("0"));

      JavaPairRDD<String, String> sorted = timeFilter.sortByKey(new TupleComparator(), true);

	    sorted.saveAsTextFile(outputDataPath);

	    sc.close();
	  }
}

class TupleComparator implements Comparator<String>, Serializable{
  @Override
  public int compare(String o1, String o2) {
    if (o1.split("; ")[0].equals(o2.split("; ")[0])){
      String per1 = o1.split("; ")[1].split(", ")[1].substring(0,o1.split("; ")[1].split(", ")[1].length()-1);
      String per2 = o2.split("; ")[1].split(", ")[1].substring(0,o2.split("; ")[1].split(", ")[1].length()-1);
      return Double.parseDouble(per1) > Double.parseDouble(per2) ? -1:1;
    }else{
      return o2.split("; ")[0].compareTo(o1.split("; ")[0]);
    }
  }
}
