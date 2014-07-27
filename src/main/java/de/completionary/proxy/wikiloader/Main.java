package de.completionary.proxy.wikiloader;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.thrift.async.AsyncMethodCallback;

import de.completionary.proxy.elasticsearch.SuggestionIndex;
import de.completionary.proxy.thrift.services.admin.SuggestionField;
import de.completionary.proxy.wikiloader.Helper.ImportScript;

public class Main {

    public static void main(String[] args) throws InterruptedException,
            ExecutionException {
        final List<SuggestionField> terms = ImportScript.loadServerWiki(1000);

        int bytesStored = 0;
        for (SuggestionField field : terms) {
            bytesStored += field.output.length() + field.payload.length();
        }
        final int fBytesStored = bytesStored;

        SuggestionIndex.delete("index");
        SuggestionIndex client = SuggestionIndex.getIndex("wikipediaindex");
        try {
            client.truncate();
            long startTime = System.currentTimeMillis();

            client.async_addTerms(terms, new AsyncMethodCallback<Long>() {

                @Override
                public void onError(Exception e) {
                    e.printStackTrace();
                }

                @Override
                public void onComplete(Long time) {
                    System.out.println("Added " + terms.size()
                            + " terms within " + time + " ms with about "
                            + fBytesStored / 1000 + " kBytes");
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
        client.waitForGreen();
    }

}
