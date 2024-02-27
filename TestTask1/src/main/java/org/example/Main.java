package org.example;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Main {
    public static void main(String[] args) {
        // Проверяем, что передан аргумент с URL-адресом файла
//        if (args.length < 1) {
//            System.out.println("Необходимо указать URL-адрес файла в аргументах командной строки.");
//            return;
//            }

        String filePath = "/home/mikhailvasilev/TestTask/lng.txt";
//      String fileUrl = "https://github.com/PeacockTeam/new-job/releases/download/v1.0/lng-4.txt.gz";
        try {
//            URL url = new URL(fileUrl);
//            ReadableByteChannel channel = Channels.newChannel(url.openStream());
//            BufferedReader reader = new BufferedReader(new InputStreamReader
//                    (Channels.newInputStream(channel), StandardCharsets.UTF_8));
            BufferedReader reader = new BufferedReader(new FileReader(filePath));

            List<String> lines = new ArrayList<>();
            String lin;
            while ((lin = reader.readLine()) != null) {
                lines.add(lin);
            }

            ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

            Set<String> uniqueLines = Collections.newSetFromMap(new ConcurrentHashMap<>());
            Map<String, List<String>> groups = new HashMap<>();
            Lock lock = new ReentrantLock();

            for (String line : lines) {
                executorService.execute(() -> {
                    if (uniqueLines.contains(line)) {
                        return;
                    }

                    uniqueLines.add(line);

                    boolean foundGroup = false;
                    lock.lock();
                    try {
                        for (String groupKey : groups.keySet()) {
                            List<String> group = groups.get(groupKey);
                            if (hasCommonValues(line, group)) {
                                group.add(line);
                                foundGroup = true;
                                break;
                            }
                        }

                        if (! foundGroup) {
                            List<String> newGroup = new ArrayList<>();
                            newGroup.add(line);
                            groups.put(line, newGroup);
                        }
                    } finally {
                        lock.unlock();
                    }
                });
            }

            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.MINUTES);

            List<List<String>> sortedGroups = new ArrayList<>(groups.values());
            sortedGroups.sort((g1, g2) -> Integer.compare(g2.size(), g1.size()));

            Path outputPath = Path.of("output.txt");

            try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                int groupCount = 0;
                for (List<String> group : sortedGroups) {
                    if (group.size() > 1) {
                        groupCount++;
                        writer.write("Группа " + groupCount + ":\n");
                        for (String lineInGroup : group) {
                            writer.write(lineInGroup + "\n");
                        }
                        writer.write("\n");
                    }
                }
                writer.flush();
                System.out.println("Количество групп с более чем одним элементом: " + groupCount);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static boolean hasCommonValues(String line1, List<String> group) {
        String[] values1 = line1.split(";");
        for (String line2 : group) {
            String[] values2 = line2.split(";");
            for (String value1 : values1) {
                for (String value2 : values2) {
                    if (value1.equals(value2) && ! value1.isEmpty()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
