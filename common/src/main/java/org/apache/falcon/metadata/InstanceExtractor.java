/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.metadata;

import org.apache.falcon.entity.common.FeedDataPath;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;

/**
 * delete me.
 */
public class InstanceExtractor {

    private static final Logger LOG = Logger.getLogger(InstanceExtractor.class);

    private static final String FORMAT = "yyyyMMddHHmm";

    private String getDateFormatInPath(String inPath) {
        String mask = extractDatePartFromPathMask(inPath, inPath);
        //yyyyMMddHHmm
        return mask.replaceAll(FeedDataPath.VARS.YEAR.regex(), "yyyy")
                .replaceAll(FeedDataPath.VARS.MONTH.regex(), "MM")
                .replaceAll(FeedDataPath.VARS.DAY.regex(), "dd")
                .replaceAll(FeedDataPath.VARS.HOUR.regex(), "HH")
                .replaceAll(FeedDataPath.VARS.MINUTE.regex(), "mm");
    }

    private String extractDatePartFromPathMask(String mask, String inPath) {
        String[] elements = FeedDataPath.PATTERN.split(mask);

        String out = inPath;
        for (String element : elements) {
            out = out.replaceFirst(element, "");
        }
        return out;
    }

    private final Map<FeedDataPath.VARS, String> map = new TreeMap<FeedDataPath.VARS, String>();

    //consider just the first occurrence of the pattern
    private String getDateString(Path file, String inMask,
                                 String dateMask) {
        String path = extractDatePartFromPathMask(inMask, file.toString());
        System.out.println("path = " + path);
        populateDatePartMap(path, dateMask);
        System.out.println("map = " + map);

        String errArg = file + "(" + inMask + ")";
        if (map.isEmpty()) {
            LOG.warn("No date present in " + errArg);
            return null;
        }

        String date = "";
        int ordinal = 0;
        for (FeedDataPath.VARS var : map.keySet()) {
            if (ordinal++ == var.ordinal()) {
                date += map.get(var);
            } else {
                LOG.warn("Prior element to " + var + " is missing " + errArg);
                return null;
            }
        }
        System.out.println("date = " + date);

        String format = FORMAT.substring(0, date.length());
        System.out.println("format = " + format);
        return date;
    }

    private void populateDatePartMap(String path, String mask) {
        map.clear();
        Matcher matcher = FeedDataPath.DATE_FIELD_PATTERN.matcher(mask);
        int start = 0;
        while (matcher.find(start)) {
            String subMask = mask.substring(matcher.start(), matcher.end());
            String subPath = path.substring(matcher.start(), matcher.end());
            FeedDataPath.VARS var = FeedDataPath.VARS.from(subMask);
            if (!map.containsKey(var)) {
                map.put(var, subPath);
            }
            start = matcher.start() + 1;
        }
    }

    public static void main(String[] args) throws ParseException {
        InstanceExtractor extractor = new InstanceExtractor();

        String nominalTime = "2014-02-02-01-01";
        System.out.println("nominalTime = " + nominalTime);

        String feedPath = "hdfs://localhost:8020/impression-feed/${YEAR}-${MONTH}-${DAY}-${HOUR}";
        String feedInstancePath = "hdfs://localhost:8020/impression-feed/2014-01-01-01";
        String feedPathMask = feedPath.replaceAll(FeedDataPath.VARS.YEAR.regex(), "yyyy")
                .replaceAll(FeedDataPath.VARS.MONTH.regex(), "MM")
                .replaceAll(FeedDataPath.VARS.DAY.regex(), "dd")
                .replaceAll(FeedDataPath.VARS.HOUR.regex(), "HH")
                .replaceAll(FeedDataPath.VARS.MINUTE.regex(), "mm");
        System.out.println("feedPathMask = " + feedPathMask);

        String dateMask = extractor.getDateFormatInPath(feedPath);
        System.out.println("dateMask = " + dateMask);

        String foo = extractor.getDateString(new Path(feedInstancePath), feedPath, dateMask);
        System.out.println("foo = " + foo);


        System.out.println("----------------------");
        System.out.println("----------------------");

        String out = feedInstancePath;

        String[] elements = FeedDataPath.PATTERN.split(feedPath);
        for (String element : elements) {
            System.out.println("element = " + element);
            out = out.replaceFirst(element, "");
            System.out.println("out = " + out);
        }
        System.out.println("path = " + out);
        Map<FeedDataPath.VARS, String> map = new TreeMap<FeedDataPath.VARS, String>();
        Matcher matcher = FeedDataPath.DATE_FIELD_PATTERN.matcher(dateMask);
        int start = 0;
        while (matcher.find(start)) {
            System.out.println("start = " + start);
            String subMask = dateMask.substring(matcher.start(), matcher.end());
            System.out.println("subMask = " + subMask);
            String subPath = out.substring(matcher.start(), matcher.end());
            System.out.println("subPath = " + subPath);
            FeedDataPath.VARS var = FeedDataPath.VARS.from(subMask);
            System.out.println("var = " + var);
            if (!map.containsKey(var)) {
                map.put(var, subPath);
            }
            start = matcher.start() + 1;
        }

        System.out.println("map = " + map);

        String date = "";
        int ordinal = 0;
        for (FeedDataPath.VARS var : map.keySet()) {
            if (ordinal++ == var.ordinal()) {
                date += map.get(var);
                System.out.println("date = " + date);
            } else {
                LOG.warn("Prior element to " + var + " is missing " + "xxx");
            }
        }
        System.out.println("instance = " + date);


//        Date blah = extractor.getDate(new Path(feedInstancePath), feedPath, dateMask, "UTC");
//        System.out.println("blah = " + blah);



/*
        String timezone = "UTC";
        TimeZone timeZone = TimeZone.getTimeZone(timezone);
        System.out.println("timeZone = " + timeZone);
        DateFormat dateFormat = new SimpleDateFormat(FORMAT);
        dateFormat.setTimeZone(timeZone);
        Date nominalTimeDate = dateFormat.parse(nominalTime);
        System.out.println("nominalTimeDate = " + nominalTimeDate);
        String format = dateFormat.format(nominalTimeDate);
        System.out.println("format = " + format);

        DateFormat feedDateFormat = new SimpleDateFormat(dateMask);
        feedDateFormat.setTimeZone(timeZone);
        Date feedDate = feedDateFormat.parse(nominalTime);
        System.out.println("feedDate = " + feedDate);

        String feedInstance = feedDateFormat.format(feedDate);
        System.out.println("feedInstance = " + feedInstance);

        Date feedsDate = extractor.getDate(new Path(feedInstancePath), feedPath, dateMask, timezone);
        System.out.println("feedDate = " + feedsDate);

        String instance = feedDateFormat.format(feedsDate);
        System.out.println("instance = " + instance);
*/
    }
}
