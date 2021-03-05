package no.ssb.rawdata.converter.app.migration;

import org.apache.commons.text.CaseUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class AvroUtils {

    static final char[] ILLEGAL_AVRO_CHARS;

    static {
        List<Character> illegalChars = new ArrayList<>();
        IntStream.range(33, 47 + 1).forEach(i -> illegalChars.add((char) i));
        IntStream.range(58, 64 + 1).forEach(i -> illegalChars.add((char) i));
        IntStream.range(91, 96 + 1).forEach(i -> illegalChars.add((char) i));
        IntStream.range(123, 126 + 1).forEach(i -> illegalChars.add((char) i));
        ILLEGAL_AVRO_CHARS = new char[illegalChars.size()];
        for (int i = 0; i < illegalChars.size(); i++) ILLEGAL_AVRO_CHARS[i] = illegalChars.get(i);
    }

    static public String formatToken(String str) {
        return CaseUtils.toCamelCase(removeChars(str, "\\?"), true, ILLEGAL_AVRO_CHARS); // avro mappings
    }

    static public String removeChars(String str, String... chars) {
        List<String> removeChars = new ArrayList<>(List.of(chars));
        removeChars.add("\t");
        removeChars.add("\r");
        removeChars.add("\n");
        for (String ch : removeChars) {
            str = str.replaceAll(ch, "");
        }
        return str;
    }
}
