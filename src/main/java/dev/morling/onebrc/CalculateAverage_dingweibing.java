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
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class CalculateAverage_dingweibing {

    private static final Path PATH = Path.of("./measurements.txt");

    public static void main(String[] args) throws IOException, InterruptedException {
        final long fileSize = Files.size(PATH);
        final int chunkCount = Runtime.getRuntime().availableProcessors();
        final long[] chunkStartOffsets = new long[chunkCount];
        final StationStats[][] results = new StationStats[chunkCount][];
        try (RandomAccessFile raf = new RandomAccessFile(PATH.toFile(), "r");
             ExecutorService executor = Executors.newFixedThreadPool(chunkCount)) {
            for (int i = 1; i < chunkCount; i++) {
                long start = fileSize * i / chunkCount;
                raf.seek(start);
                while (raf.read() != '\n' && start < fileSize) {
                    start++;
                }
                chunkStartOffsets[i] = start;
            }

            MemorySegment memorySegment = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());
            BiPredicate<Integer, long[]> isLastChunk = (idx, chunks) -> chunks.length == idx + 1;
            for (int i = 0; i < chunkCount; i++) {
                long chunkStart = chunkStartOffsets[i];
                long chunkLimit = isLastChunk.test(i, chunkStartOffsets) ? fileSize : chunkStartOffsets[i + 1];
                executor.execute(new ChunkProcessor(memorySegment.asSlice(chunkStart, chunkLimit - chunkStart), results, i));
            }
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        }
    }

    private static class ChunkProcessor implements Runnable {
        private final MemorySegment chunk;
        private final StationStats[][] results;
        private final int myIndex;
        private final Map<String, StationStats> statsMap = new HashMap<>();

        ChunkProcessor(MemorySegment chunk, StationStats[][] results, int myIndex) {
            this.chunk = chunk;
            this.results = results;
            this.myIndex = myIndex;
        }

        @Override
        public void run() {
            for (var cursor = 0L; cursor < chunk.byteSize(); ) {
                var semicolonPos = findByte(cursor, ';');
                var newlinePos = findByte(semicolonPos + 1, '\n');
                var name = stringAt(cursor, semicolonPos);
                // Variant 1:
//                var temp = Double.parseDouble(stringAt(semicolonPos + 1, newlinePos));
//                var intTemp = (int) Math.round(10 * temp);

                // Variant 2:
                var intTemp = parseTemperature(semicolonPos);

                var stats = statsMap.computeIfAbsent(name, k -> new StationStats(name));
                stats.sum += intTemp;
                stats.count++;
                stats.min = Math.min(stats.min, intTemp);
                stats.max = Math.max(stats.max, intTemp);
                cursor = newlinePos + 1;
            }
            results[myIndex] = statsMap.values().toArray(StationStats[]::new);
        }

        private int parseTemperature(long semicolonPos) {
            long off = semicolonPos + 1;
            int sign = 1;
            byte b = chunk.get(JAVA_BYTE, off++);
            if (b == '-') {
                sign = -1;
                b = chunk.get(JAVA_BYTE, off++);
            }
            int temp = b - '0';
            b = chunk.get(JAVA_BYTE, off++);
            if (b != '.') {
                temp = 10 * temp + b - '0';
                // we found two integer digits. The next char is definitely '.', skip it:
                off++;
            }
            b = chunk.get(JAVA_BYTE, off);
            temp = 10 * temp + b - '0';
            return sign * temp;
        }

        private long findByte(long cursor, int b) {
            for (var i = cursor; i < chunk.byteSize(); i++) {
                if (chunk.get(JAVA_BYTE, i) == b) {
                    return i;
                }
            }
            throw new RuntimeException(((char) b) + " not found");
        }

        private String stringAt(long start, long limit) {
            return new String(
                    chunk.asSlice(start, limit - start).toArray(JAVA_BYTE),
                    StandardCharsets.UTF_8
            );
        }
    }

    static class StationStats implements Comparable<StationStats> {
        String name;
        long sum;
        int count;
        int min;
        int max;

        StationStats(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min / 10.0, Math.round((double) sum / count) / 10.0, max / 10.0);
        }

        @Override
        public boolean equals(Object that) {
            return that.getClass() == StationStats.class && ((StationStats) that).name.equals(this.name);
        }

        @Override
        public int compareTo(StationStats that) {
            return name.compareTo(that.name);
        }
    }
}
