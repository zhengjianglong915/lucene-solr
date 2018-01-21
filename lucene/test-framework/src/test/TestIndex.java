/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * Created by zhengjianglong on 2018/1/21.
 */

public class TestIndex {
  public static void main(String[] args) {
    // 索引文件存储位置
    String indexPath = "/Users/zhengjianglong/workplace/temp/index";
    // 需要进行索引的文件
    String docsPath = "/Users/zhengjianglong/workplace/temp/test.txt";
    final Path docDir = Paths.get(docsPath);
    Date start = new Date();
    try {
      System.out.println("Indexing to directory '" + indexPath + "'...");
      // 索引文件存储目录
      Directory dir = FSDirectory.open(Paths.get(indexPath));
      // 指定分析器
      Analyzer analyzer = new StandardAnalyzer();
      // 设置索引的配置
      IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
      // 构建IndexWriter 实例
      IndexWriter writer = new IndexWriter(dir, iwc);
      indexDoc(writer, docDir, Files.getLastModifiedTime(docDir).toMillis());
      writer.close();
      Date end = new Date();
      System.out.println(end.getTime() - start.getTime() + " total milliseconds");

    } catch (IOException e) {
      System.out.println(" caught a " + e.getClass() +
          "\n with message: " + e.getMessage());
    }
  }
  /**
   * Indexes a single document
   */
  static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
    try (InputStream stream = Files.newInputStream(file)) {
      // make a new, empty document
      // 创建一个Document对象
      Document doc = new Document();
      Field pathField = new StringField("path", file.toString(), Field.Store.YES);
      doc.add(pathField);
      doc.add(new LongPoint("modified", lastModified));
      doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));
      System.out.println("adding " + file);
      writer.addDocument(doc);
    }
  }
}
