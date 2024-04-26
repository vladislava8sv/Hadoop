import java.io.*;
import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class WordMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

  private static final int WORD_LENGTH_TO_COUNT = 5; // Указать длину слова для подсчета

  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
        Analyzer analyzer = new RussianAnalyzer();
        TokenStream tokenStream = analyzer.tokenStream("contents", new StringReader(value.toString()));
        tokenStream.reset();
        CharTermAttribute term = tokenStream.addAttribute(CharTermAttribute.class);
        while(tokenStream.incrementToken()) {
            String word = term.toString();
            // Проверяем длину слова и отправляем на выход
            if (word.length() == WORD_LENGTH_TO_COUNT) {
                context.write(new IntWritable(word.length()), new IntWritable(1));
            }
        }
  }
}
