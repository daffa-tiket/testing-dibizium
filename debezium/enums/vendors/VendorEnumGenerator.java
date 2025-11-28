package com.tiket.tix.hotel.common.debezium.enums.vendors;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class VendorEnumGenerator {

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.out.println("Usage: java VendorEnumGenerator <package.name> <vendors.txt path>");
      return;
    }

    String folderName = args[0];
    String packageName = args[1];
    Path input = Paths.get(args[2]);

    List<String> vendors = Files.readAllLines(input);

    Path folder = Paths.get(folderName + "/src/main/java", packageName.replace('.', '/'));
    Files.createDirectories(folder);

    Path file = folder.resolve("Vendors.java");

    if (Files.exists(file)) {
      Files.delete(file);
      System.out.println("Vendors Enum Reset: " + file.toAbsolutePath());
    }

    try (BufferedWriter writer = Files.newBufferedWriter(file)) {
      writer.write("package " + packageName + ";\n\n");
      writer.write("public enum Vendors {\n");

      for (int i = 0; i < vendors.size(); i++) {
        String vendor = vendors.get(i).trim().toUpperCase();
        if (vendor.isEmpty()) continue;

        writer.write("    " + vendor);
        if (i < vendors.size() - 1) writer.write(",");
        writer.write("\n");
      }

      writer.write(";\n}");
    }

    System.out.println("✅ Vendors enum generated successfully at: " + folder.toAbsolutePath());
  }
}
