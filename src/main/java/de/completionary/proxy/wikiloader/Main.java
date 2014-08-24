package de.completionary.proxy.wikiloader;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.thrift.async.AsyncMethodCallback;

import de.completionary.proxy.elasticsearch.SuggestionIndex;
import de.completionary.proxy.thrift.services.admin.SuggestionField;
import de.completionary.proxy.thrift.services.exceptions.IndexAlreadyExistsException;
import de.completionary.proxy.thrift.services.exceptions.IndexUnknownException;
import de.completionary.proxy.thrift.services.exceptions.InvalidIndexNameException;
import de.completionary.proxy.thrift.services.exceptions.ServerDownException;
import de.completionary.proxy.wikiloader.Helper.ImportScript;

public class Main {

    public static void main(String[] args) throws InterruptedException,
            ExecutionException {
        String index = "wikipediaindex";
        final List<SuggestionField> terms = ImportScript.loadServerWiki(15000);

        int bytesStored = 0;
        for (SuggestionField field : terms) {
            bytesStored += field.outputField.length() + field.payload.length();
        }
        final int fBytesStored = bytesStored;

        // SuggestionIndex.delete(index);
        SuggestionIndex client = null;
        try {
            try {
                client = SuggestionIndex.getIndex(index);
            } catch (IndexUnknownException e1) {
                try {
                    client = SuggestionIndex.generateIndex(index);
                } catch (IndexAlreadyExistsException e2) {
                }
            }

            client.truncate();
            // for (SuggestionField field : terms) {
            // final CountDownLatch lock = new CountDownLatch(1);
            // client.async_addSingleTerm(field.ID, field.input,
            // field.outputField, field.payload, field.weight,
            // new AsyncMethodCallback<Long>() {
            //
            // @Override
            // public void onError(Exception e) {
            // e.printStackTrace();
            // lock.countDown();
            // }
            //
            // @Override
            // public void onComplete(Long time) {
            // lock.countDown();
            // }
            // });
            // lock.await(2000, TimeUnit.MILLISECONDS);
            // }
            System.out.println("Added " + terms.size() + " terms with about "
                    + fBytesStored / 1000 + " kBytes");

            final CountDownLatch lock = new CountDownLatch(1);
            client.async_addTerms(terms, new AsyncMethodCallback<Long>() {

                @Override
                public void onError(Exception e) {
                    e.printStackTrace();
                    lock.countDown();
                }

                @Override
                public void onComplete(Long time) {
                    System.out.println("Added " + terms.size()
                            + " terms within " + time + " ms with about "
                            + fBytesStored / 1000 + " kBytes");
                    lock.countDown();
                }
            });
            lock.await();
        } catch (InvalidIndexNameException | ServerDownException e1) {
            e1.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
