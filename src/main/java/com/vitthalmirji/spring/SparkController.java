package com.vitthalmirji.spring;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import java.awt.Color;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;



import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;




import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


@RequestMapping("Home")
@Controller
public class SparkController {
    @Autowired
    private SparkSession sparkSession;

    @RequestMapping("Get_Data")
    public ResponseEntity<String> getData() {
        Dataset<Row> wazzaaf_job = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
        Dataset<Row> wazzaaf_NoNull_NoDublicates = wazzaaf_job.distinct().filter("YearsExp NOT LIKE 'null%'");
        String html = String.format("<h1>%s</h1>", "Running Apache Spark on/with support of Spring boot") +
                String.format("<h2>%s</h2>", "Spark version = "+sparkSession.sparkContext().version()) +
                String.format("<h3>%s</h3>", "Read Wazzaf data:") +
                String.format("<h4>Schema</h4>" + wazzaaf_job.schema().toString())+
                wazzaaf_NoNull_NoDublicates.showString(20, 20, true);
        return ResponseEntity.ok(html);

    }
    @RequestMapping("Top_jops")
    public ResponseEntity<String> GetJobs() {
        Dataset<Row> wazzaaf_job = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
        Dataset<Row> wazzaaf_NoNull_NoDublicates = wazzaaf_job.distinct().filter("YearsExp NOT LIKE 'null%'");
        Dataset<Row> jopssorted_Count = Count_Data(wazzaaf_NoNull_NoDublicates,"Title");

        String html = String.format("<h3>%s</h3>", "Display top Jobs!")+
                jopssorted_Count.showString(20, 20, true);

        return ResponseEntity.ok(html);

    }

    @RequestMapping("Top_Companies")
    public ResponseEntity<String> GetCompanies() {
        Dataset<Row> wazzaaf_job = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
        Dataset<Row> wazzaaf_NoNull_NoDublicates = wazzaaf_job.distinct().filter("YearsExp NOT LIKE 'null%'");
        Dataset<Row> companySorted_count =Count_Data(wazzaaf_NoNull_NoDublicates,"Company");

        String html =  String.format("<h3>%s</h3>", "Display top companies:")+
                companySorted_count.showString(20, 20, true);

        return ResponseEntity.ok(html);

    }

    @RequestMapping("Top_Areas")
    public ResponseEntity<String> GetAreas() {
        Dataset<Row> wazzaaf_job = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
        Dataset<Row> wazzaaf_NoNull_NoDublicates = wazzaaf_job.distinct().filter("YearsExp NOT LIKE 'null%'");
        Dataset<Row> areasorted_Count = Count_Data(wazzaaf_NoNull_NoDublicates,"Location");

        String html =  String.format("<h3>%s</h3>", "Display top Areas!")+
                areasorted_Count.showString(20, 20, true);

        return ResponseEntity.ok(html);

    }
    @RequestMapping("Most_Imp_Skills")
    public ResponseEntity<String> GetSkillsImportant() {
        Dataset<Row> wazzaaf_job = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
        Dataset<Row> wazzaaf_NoNull_NoDublicates = wazzaaf_job.distinct().filter("YearsExp NOT LIKE 'null%'");
        String html = String.format("<h3>%s</h3>", "Display The Most Important skills!") +
                Most_Important_Skills(wazzaaf_NoNull_NoDublicates).toString();

        return ResponseEntity.ok(html);
    }







    static public Dataset<Row> Count_Data(Dataset<Row> df ,String Col_Name){

        Dataset<Row> result = df.groupBy(Col_Name).agg(functions.count(Col_Name).as("Count")).sort(functions.desc("Count"));
        return result ;
    }
    static public LinkedHashMap<String, Long> Most_Important_Skills (Dataset<Row> data){
        List list_skills = new ArrayList<>();

        data.select("Skills").collectAsList().forEach(r -> {
            list_skills.add(r);
        });
        List<String> Splited_skills = new ArrayList<>();
        for(int i = 0; i< list_skills.size(); i++){
            Splited_skills.addAll(Arrays.asList(((String)((Row)list_skills.get(i)).get(0)).split(",")));
        }

        Map<String, Long> Count_Map =
                Splited_skills.stream().collect(Collectors.groupingBy(Function.identity(),Collectors.counting()));


        //LinkedHashMap preserve the ordering of elements in which they are inserted
        LinkedHashMap<String, Long> reverseSortedMap = new LinkedHashMap<>();

        //Use Comparator.reverseOrder() for reverse ordering
        Count_Map.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));

        //System.out.println("Reverse Sorted Map   : " + reverseSortedMap);
        return reverseSortedMap;
}
}
