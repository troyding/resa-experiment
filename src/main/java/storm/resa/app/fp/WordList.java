package storm.resa.app.fp;

import java.util.Arrays;

/**
 * Created by ding on 14-6-5.
 */
public class WordList {

    private int[] words;

    public WordList(int[] words) {
        this.words = words;
    }

    public int[] getWords() {
        return words;
    }

    @Override
    public int hashCode() {
        return words == null ? 0 : Arrays.hashCode(words);
    }

    @Override
    public boolean equals(Object obj) {
        return Arrays.equals(words, ((WordList) obj).words);
    }
}
