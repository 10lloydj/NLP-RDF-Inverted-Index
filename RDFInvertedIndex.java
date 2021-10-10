/** 
 *
 * Copyright (c) University of Manchester - All Rights Reserved
 * Unauthorised copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Kristian Epps <kristian@xepps.com>, August 28, 2013
 * 
 * RDF Inverted Index
 * 
 * This Map Reduce program should read in a set of RDF/XML documents and output
 * the data in the form:
 * 
 * {predicate, object]}, [subject1, subject2, ...] 
 * 
 * @author Kristian Epps
 * 
 */
package uk.ac.man.cs.comp38211.exercise;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.rdf.model.Property;

import uk.ac.man.cs.comp38211.io.array.ArrayListWritable;
import uk.ac.man.cs.comp38211.io.pair.PairOfStrings;
import uk.ac.man.cs.comp38211.util.XParser;

public class RDFInvertedIndex extends Configured implements Tool
{
    private static final Logger LOG = Logger
            .getLogger(RDFInvertedIndex.class);
//Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class Map extends 
            Mapper<LongWritable, Text, Text, PairOfStrings>
    {        

        protected Text document = new Text();
        protected Text predobj = new Text();
        protected Text subj = new Text();
        protected PairOfStrings res = new PairOfStrings();
//map(KEYIN key, VALUEIN value, org.apache.hadoop.mapreduce.Mapper.Context context)        
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {            
            // This statement ensures we read a full rdf/xml document in
            // before we try to do anything else
            if(!value.toString().contains("</rdf:RDF>"))
            {    
                document.set(document.toString() + value.toString());
                return;
            } // if
            
            // We have to convert the text to a UTF-8 string. This must be
            // enforced or errors will be thrown. 
            String contents = document.toString() + value.toString();
            contents = new String(contents.getBytes(), "UTF-8");
            
            // The string must be cast to an inputstream for use with jena
            InputStream fullDocument = IOUtils.toInputStream(contents);
            document = new Text();
            
            // Create a model
            Model model = ModelFactory.createDefaultModel();
            
            String mapValue;
            // position index counter
            int pos = 0;
            String stPos;
            try
            {
                model.read(fullDocument, null);
            
                StmtIterator iter = model.listStatements();
            
                // Iterate over all the triples, set and output them
                while(iter.hasNext())
                {
                	// count for statements position in each rdf file
                	pos++;
                	// string conversion
                	stPos= pos + "";
                    Statement stmt 		= iter.nextStatement();
                    // actual entity
                    Resource  subject   = stmt.getSubject();
                    String ssubj = subject.toString();
                    // topic
                    Property  predicate = stmt.getPredicate();
                    //value
                    RDFNode   object    = stmt.getObject(); 
                    String stobj = object.toString();
                    // temp variables
                    String sto1, sto2;
                    
                    // appends local name predicate with the statement position
                    String localName = predicate.getLocalName() + ",(" + stPos + ")";
                    
                    int lastSlash;
                    
 //---------------------Data Cleaning ---------------------------------------//                    
                    // removes @en from the string
                    stobj = stobj.replaceAll("@en", "");
                    // removes quotation marks at the beginning of the object, ensuring better ordering
                    stobj = stobj.replaceAll("^\"|\"$", "");
                    // replace hyphen and underscores with space
                    stobj = stobj.replaceAll("-", " ");
                    stobj = stobj.replaceAll("_", " ");
                    // removes URI after dates 
                    if(stobj.contains("^^"))
                    	stobj = stobj.split("\\^\\^")[0];
                    // removed URI till last forward slash
                    // removes http URl from object
                    if(stobj.contains("http")) {
                    	lastSlash = stobj.lastIndexOf("/");
                    	sto1 = stobj.substring(lastSlash+1);
                    	sto1.trim();
                    	stobj = sto1;
                    } // if
                    // removes http URl from resource
                    if(ssubj.contains("http")) {
                    	lastSlash = ssubj.lastIndexOf("/");
                    	sto2 = ssubj.substring(lastSlash+1);
                    	sto2.trim();
                    	ssubj = sto2;
                    } // if 
                    predobj.set(stobj);

                    // skips conditions from emission
                    if(localName.contains("comment") || localName.contains("wikiPageRedirects") || stobj.contains("m.0")) {}
                    else{
                    //res.set(localName, ssubj);
                    	res.set(localName, ssubj);
                    //context.write(predobj, res);
                    	context.write(predobj, res);

                    } // else
                } // while
            } // try
            catch(Exception e)
            {
                LOG.error(e);
            } // catch
        } // map
    }
//Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class Reduce extends Reducer<Text, PairOfStrings, Text, ArrayListWritable<PairOfStrings>>
    {
       
        // This reducer turns an iterable into an ArrayListWritable, sorts it
        // and outputs it
    	//	reduce(KEYIN key, Iterable<VALUEIN> values, org.apache.hadoop.mapreduce.Reducer.Context context)    	
        public void reduce(
                Text key,
                Iterable<PairOfStrings> values,
                Context context) throws IOException, InterruptedException
        {
            ArrayListWritable<PairOfStrings> postings = new ArrayListWritable<PairOfStrings>();
            
            Iterator<PairOfStrings> iter = values.iterator();

            while(iter.hasNext()) {
            	PairOfStrings str = iter.next();
            	PairOfStrings copy = new PairOfStrings(str.getKey(), str.getValue());
                postings.add(copy);
            } // while
            
            Collections.sort(postings);
            
            context.write(key, postings);
        } // reduce
    } // Reduce

    public RDFInvertedIndex()
    {
    } // RDFInvertedIndex()

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";

    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception
    {        
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));

        CommandLine cmdline = null;
        CommandLineParser parser = new XParser(true);

        try
        {
            cmdline = parser.parse(options, args);
        } // try
        catch (ParseException exp)
        {
            System.err.println("Error parsing command line: "
                    + exp.getMessage());
            System.err.println(cmdline);
            return -1;
        } // try

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT))
        {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        } // if
        
        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);

        Job RDFIndex = new Job(new Configuration());

        RDFIndex.setJobName("Inverted Index 1");
        RDFIndex.setJarByClass(RDFInvertedIndex.class);        
        RDFIndex.setMapperClass(Map.class);
        RDFIndex.setReducerClass(Reduce.class);
        RDFIndex.setMapOutputKeyClass(Text.class);
        //changed from pos
        RDFIndex.setMapOutputValueClass(PairOfStrings.class);
        RDFIndex.setOutputKeyClass(Text.class);
        RDFIndex.setOutputValueClass(ArrayListWritable.class);

        FileInputFormat.setInputPaths(RDFIndex, new Path(inputPath));
        FileOutputFormat.setOutputPath(RDFIndex, new Path(outputPath));

        long startTime = System.currentTimeMillis();
       
        RDFIndex.waitForCompletion(true);
        if(RDFIndex.isSuccessful())
            LOG.info("Job successful!");
        else
            LOG.info("Job failed.");
        
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
                / 1000.0 + " seconds");

        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        ToolRunner.run(new RDFInvertedIndex(), args);
    }
}
