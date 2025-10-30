package org.apache.hadoop.security.bzl.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ipc.metrics.RpcBzlTokenPasswordFetcherMetrics;
import org.apache.hadoop.security.bzl.bean.BzlPasswordBean;
import org.apache.hadoop.security.bzl.bean.BzlPasswordBean.PasswordData;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_BZL_AUTH_SYSTEM_ACCOUNT_LIST;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_BZL_AUTH_SYSTEM_ACCOUNT_LIST_DEFAULT;

public class BzlAuthFileUtils {
  static final Logger LOG = LoggerFactory.getLogger(BzlAuthFileUtils.class);

  public static ArrayList<String> getSystemAccountList() {
    String accounts = BzlDynamicConfiguration.getInstance()
        .get(HADOOP_BZL_AUTH_SYSTEM_ACCOUNT_LIST, HADOOP_BZL_AUTH_SYSTEM_ACCOUNT_LIST_DEFAULT);

    return Arrays.stream(accounts.split(",")).map(String::trim).filter(StringUtils::isNotBlank)
        .collect(Collectors.toCollection(ArrayList::new));
  }

  public static boolean checkSystemAccount(Map passwordMap) {
    for (String systemAccount : getSystemAccountList()) {
      if (passwordMap.get(systemAccount) == null) {
        LOG.error("Not found system account {}, do not update passwords.", systemAccount);
        return false;
      }
    }
    return true;
  }

  public static boolean checkSystemAccount(List<PasswordData> passwordList) {
    for (String systemAccount : getSystemAccountList()) {
      boolean accountExists = passwordList.stream().anyMatch(
          passwordData -> passwordData.getGroupAccount() != null && passwordData.getGroupAccount()
              .equals(systemAccount));

      if (!accountExists) {
        LOG.error("Not found system account {}, do not fetch passwords.", systemAccount);
        return false;
      }
    }
    return true;
  }

  public static void writePasswordFile(List<BzlPasswordBean.PasswordData> list, String filename,
      RpcBzlTokenPasswordFetcherMetrics rpcBzlTokenPasswordFetcherMetrics) throws IOException {

    try {
      Gson gson = new Gson();
      try (FileWriter writer = new FileWriter(filename)) {
        gson.toJson(list, writer);
      }

      writeMD5File(filename);
      rpcBzlTokenPasswordFetcherMetrics.incrBzlTokenPasswordWriteFileSuccesses();
    } catch (IOException e) {
      LOG.error("Failed to write passwords to file.", e);
      rpcBzlTokenPasswordFetcherMetrics.incrBzlTokenPasswordWriteFileFailures();
      throw e;
    }
  }

  private static void writeMD5File(String filename) throws IOException {
    try {
      Path filePath = Paths.get(filename);
      Path md5FilePath = Paths.get(filename + ".md5");

      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] fileBytes = Files.readAllBytes(filePath);
      byte[] digest = md.digest(fileBytes);

      StringBuilder sb = new StringBuilder();
      for (byte b : digest) {
        sb.append(String.format("%02x", b));
      }

      Files.write(md5FilePath, sb.toString().getBytes());
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IOException("The MD5 check value cannot be generated.", e);
    }
  }

  public static List<BzlPasswordBean.PasswordData> readPasswordFile(String filename)
      throws IOException {
    Gson gson = new Gson();
    try (FileReader reader = new FileReader(filename)) {
      Type listType = new TypeToken<List<PasswordData>>() {
      }.getType();
      return gson.fromJson(reader, listType);
    }
  }

  private static boolean verifyFileWithMD5(String filename) {
    try {
      Path filePath = Paths.get(filename);
      Path md5FilePath = Paths.get(filename + ".md5");

      if (!Files.exists(md5FilePath)) {
        LOG.warn("File {} is not exists.", md5FilePath);
        return false;
      }

      String storedMD5 = new String(Files.readAllBytes(md5FilePath)).trim();

      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] fileBytes = Files.readAllBytes(filePath);
      byte[] digest = md.digest(fileBytes);

      StringBuilder currentMD5Builder = new StringBuilder();
      for (byte b : digest) {
        currentMD5Builder.append(String.format("%02x", b));
      }
      String currentMD5 = currentMD5Builder.toString();

      if (!storedMD5.equals(currentMD5)) {
        LOG.warn("File {} is damaged.", filePath);
        return false;
      }

      return true;
    } catch (Exception e) {
      LOG.warn("The md5 verification of the file {} failed:", filename, e);
      return false;
    }
  }


  public static List<Path> findLatestGroupPasswordFileAndRetainMaxVersions(String directory,
      String specifiedGroupPassword, String prefix, int maxVersions) throws IOException {
    List<Path> paths = new ArrayList<>();
    List<Path> md5Paths = new ArrayList<>();
    Path dirPath = Paths.get(directory);

    Path specifiedPath = dirPath.resolve(specifiedGroupPassword);
    if (Files.exists(specifiedPath) && Files.isRegularFile(specifiedPath) && verifyFileWithMD5(
        specifiedPath.toString())) {
      paths.add(specifiedPath);
      md5Paths.add(Paths.get(specifiedPath + ".md5"));
    }

    List<Path> allFiles;
    try (Stream<Path> filesStream = Files.list(dirPath)) {
      allFiles = filesStream.collect(Collectors.toList());
    }

    List<Path> prefixFiles =
        allFiles.stream().filter(path -> !path.getFileName().toString().endsWith(".md5"))
            .filter(path -> isValidPasswordFileName(path, prefix)).filter(Files::isRegularFile)
            .filter(path -> {
              Path md5Path = Paths.get(path + ".md5");
              return Files.exists(md5Path);
            }).filter(path -> verifyFileWithMD5(path.toString())).sorted(Comparator.reverseOrder())
            .collect(Collectors.toList());

    for (Path file : prefixFiles) {
      if (paths.size() < maxVersions) {
        paths.add(file);
        md5Paths.add(Paths.get(file + ".md5"));
      }
    }

    allFiles.stream().filter(path -> !path.getFileName().toString().endsWith(".md5"))
        .filter(path -> isValidPasswordFileName(path, prefix))
        .filter(path -> !paths.contains(path) && !md5Paths.contains(path)).forEach(path -> {
          try {
            Files.deleteIfExists(path);
            Files.deleteIfExists(Paths.get(path + ".md5"));
          } catch (IOException e) {
            LOG.warn("Failed to delete unused password file: {}", path, e);
          }
        });

    return paths;
  }

  private static boolean isValidPasswordFileName(Path path, String prefix) {
    try {
      String fileName = path.getFileName().toString();
      if (!fileName.startsWith(prefix)) {
        return false;
      }

      if (fileName.length() > prefix.length()) {
        String timePart = fileName.substring(prefix.length());
        return isValidDateTimeFormat(timePart);
      }

      return false;
    } catch (Exception e) {
      LOG.warn("Failed to validate password file name format: {}", path, e);
      return false;
    }
  }

  private static boolean isValidDateTimeFormat(String timeStr) {
    if (timeStr == null || timeStr.length() != 14) {
      return false;
    }

    try {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
      LocalDateTime.parse(timeStr, formatter);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}