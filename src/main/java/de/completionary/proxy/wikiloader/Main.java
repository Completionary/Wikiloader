package de.completionary.proxy.wikiloader;

import java.io.IOException;
import java.util.List;

import de.completionary.proxy.elasticsearch.SuggestionIndex;
import de.completionary.proxy.structs.SuggestionField;
import de.completionary.proxy.wikiloader.Helper.ImportScript;

public class Main {

    public static void main(String[] args) {
        List<SuggestionField> terms = ImportScript.loadServerWiki(10000);

        int bytesStored = 0;
        for (SuggestionField field : terms) {
            bytesStored += field.output.length() + field.payload.length();
        }

        SuggestionIndex client = new SuggestionIndex("index");
        try {
            long startTime = System.currentTimeMillis();
            client.addTerms(terms);
            long time = System.currentTimeMillis() - startTime;
            System.out
                    .println("Added " + terms.size() + " terms within " + time
                            + " ms with about " + bytesStored / 1000
                            + " kBytes");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
