/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.DoubleSummaryStatistics;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summarizingDouble;

public class CalculateAverage_dingweibing {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws IOException {
        try (Stream<String> lines = Files.lines(Path.of(FILE))) {
            Map<String, DoubleSummaryStatistics> allStats = lines
                    .parallel()
                    .collect(groupingBy(line -> line.substring(0, line.indexOf(";")), summarizingDouble(
                            line -> Double.parseDouble(
                                    line.substring(line.indexOf(";") + 1)))));
            Map<String, String> collect = allStats.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> {
                        var stats = e.getValue();
                        return String.format("%.1f/%.1f/%.1f",
                                stats.getMin(), stats.getAverage(), stats.getMax());
                    },
                    (l, r) -> r,
                    TreeMap::new));
            System.out.println(collect);
        }
    }
}
