package org.apache.hadoop.examples;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVReader;

public class FlightDataAnalysis {

	public static class FlightMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			// Reading Line one by one from the input CSV.
			CSVReader reader = new CSVReader(new StringReader(str));

			String[] split = reader.readNext();
			reader.close();
			Date travelDate = null;
			Date afterDate = null;
			Date beforeDate = null;
			String dest = split[17];
			String origin = split[11];
			String depTime = split[24];
			String arrTime = split[35];

			int cancelled = Math.round(Float.parseFloat(split[41]));
			int diverted = Math.round(Float.parseFloat(split[43]));
			DateFormat f = new SimpleDateFormat("yyyy-MM-dd");
			try {

				travelDate = f.parse(split[5]);
				afterDate = f.parse("2007-05-31");
				beforeDate = f.parse("2008-06-01");
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Text cKey;
			String cStr;
			Text cVal;
			if (!(origin.equals("ORD") && dest.equals("JFK"))) {
				if ((travelDate.before(beforeDate) && travelDate
						.after(afterDate)) && (cancelled == 0 && diverted == 0)) {
					if (origin.equals("ORD")) {
						cKey = new Text(split[5] + dest);
						cStr = arrTime + "," + origin + "," + dest + ","
								+ split[37];
						cVal = new Text(cStr);
						// emit record to reduce with ORD as marker of the
						// record for F1 flight
						// key here is intermediate location along with the
						// date.
						context.write(cKey, cVal);

					} else if (dest.equals("JFK")) {
						cKey = new Text(split[5] + origin);
						cStr = depTime + "," + origin + "," + dest + ","
								+ split[37];
						cVal = new Text(cStr);
						// emit record to reduce with JFK as marker of the
						// record for F2 flight.
						// key here is intermediate location along with the
						// date.
						context.write(cKey, cVal);
					}
				}
			}
		}
	}

	public static class FlightReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<String> F1FlightList = new ArrayList<String>();
			List<String> F2FlightList = new ArrayList<String>();
			for (Text c : values) {
				String C = c.toString();
				if (C.contains("ORD")) {
					F1FlightList.add(C);
				} else if (C.contains("JFK")) {
					F2FlightList.add(C);
				}
			}
			for (String f1 : F1FlightList) {
				String[] f1Split = f1.toString().split(",");
				// converts 24 hr time to Date format for ease of comparing
				SimpleDateFormat sdf = new SimpleDateFormat("HHmm");
				Date arrivalTime = null;
				try {
					arrivalTime = sdf.parse(f1Split[0]);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				int f1Delay = Math.round(Float.parseFloat(f1Split[3]));

				for (String f2 : F2FlightList) {
					String[] f2Split = f2.toString().split(",");
					Date deptTime = null;
					try {
						deptTime = sdf.parse(f2Split[0]);
					} catch (ParseException e) {
						e.printStackTrace();
					}
					int f2Delay = Math.round(Float.parseFloat(f2Split[3]));
					// check if the arrival time of F1 is before departure time
					// of F2
					if (arrivalTime.before(deptTime)) {
						int delay = f1Delay + f2Delay;
						Text finalK = new Text();
						Text finalV = new Text(Integer.toString(delay));
						// Write the intermediate value in form of null key with
						// the delay time as value
						context.write(finalK, finalV);
					}
				}
			}

		}
	}

	// Mapper for the second job of MapReduce, simply sends the output to
	// Reducer
	// no job done inside the mapper.
	public static class FlightMapper1 extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			str.replace("\\s+", "");
			context.write(new Text(), new Text(str));
		}
	}

	// Reducer for the second job of MapReduce, emits the average delay of all
	// the flights
	public static class FlightReducer1 extends Reducer<Text, Text, Text, Text> {

		float totalDelayMinutes;
		int totalFlights;

		public void setup(Context context) {
			totalDelayMinutes = 0;
			totalFlights = 0;
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text c : values) {
				float delayMin = Float.parseFloat(c.toString());
				totalDelayMinutes = totalDelayMinutes + delayMin;
				totalFlights++;
			}
		}

		// Writing the average delay of flights over the one year time period.
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			float averageDelay = totalDelayMinutes / totalFlights;
			Text out = new Text(Float.toString(averageDelay));
			context.write(null, out);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err
					.println("Usage: wordcount <in> <IntermediateOut> <FinalOut>");
			System.exit(2);
		}
		Job job = new Job(conf, "Flight Delay");
		job.setJarByClass(FlightDataAnalysis.class);
		job.setMapperClass(FlightMapper.class);
		job.setReducerClass(FlightReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(10);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Creating the new Job for the Second task.
		if (job.waitForCompletion(true)) {
			Configuration conf1 = new Configuration();
			Job job1 = new Job(conf1, "Flight Delay 1");
			job1.setJarByClass(FlightDataAnalysis.class);
			job1.setMapperClass(FlightMapper1.class);
			job1.setReducerClass(FlightReducer1.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			job1.setNumReduceTasks(1);
			// Input to second MapReduce is output for first.
			FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));
			System.exit(job1.waitForCompletion(true) ? 0 : 1);
		}
	}

}