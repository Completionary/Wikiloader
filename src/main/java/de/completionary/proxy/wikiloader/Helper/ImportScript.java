/**
 * 
 */
package de.completionary.proxy.wikiloader.Helper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.codec.binary.Base64;

import de.completionary.proxy.thrift.services.admin.SuggestionField;

/**
 * @author Rene Pickhardt
 * 
 */
public class ImportScript {

    public static List<SuggestionField> loadServerWiki(
            final int maxNumberOfElements) {
        List<SuggestionField> fields = new ArrayList<SuggestionField>();
        // articlswiththumbnail.tsv: ID \t Lemma \t image path
        // Pagerank: wiki-pr.tsv ID \t prvalue
        // path: /home/metalroot/
        // imagepath:
        // http://upload.wikimedia.org/wikipedia/commons/thumb/9/9e/Aki_Kaurism%C3%A4ki.jpg/92px-Aki_Kaurism%C3%A4ki.jpg

        String path = "/var/lib/datasets/rawdata/commons/wiki-pr.tsv";
        HashMap<Integer, Integer> prValues = new HashMap<Integer, Integer>();
        // HashMap<String, Integer> prTitles = new HashMap<String, Integer>();

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {

            String line = null;
            try {
                while ((line = br.readLine()) != null) {
                    String[] values = line.split("\t");
                    Integer id = Integer.parseInt(values[0]);
                    Double pr = Double.parseDouble(values[1]);
                    double d = pr * 1000. * 1000. * 10000.;
                    prValues.put(id, (int) d);
                }
                System.out.println("Finished reading " + prValues.size()
                        + " pagerank values");
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (IOException e4) {
            e4.printStackTrace();
        }

        String imagePath = "/var/lib/datasets/rawdata/commons/IMAGES/";
        try (BufferedReader br =
                new BufferedReader(
                        new FileReader(
                                "/var/lib/datasets/rawdata/commons/articlswiththumbnail.tsv"))) {

            String line = null;
            long suggestionID = 0;
            while ((line = br.readLine()) != null) {
                String[] values = line.split("\t");
                if (values.length != 3)
                    continue;

                String id = values[0];
                String title = values[1];
                String key = title.replace(" ", "_");
                String tmp = values[2];
                values =
                        tmp.replace(
                                "http://upload.wikimedia.org/wikipedia/commons/thumb/",
                                "").split("/");
                if (values.length != 4)
                    continue;
                String image = values[0] + "/" + values[1] + "/" + values[3];

                File f = new File(imagePath + image);
                if (!f.exists()) {
                    continue;
                }

                FileInputStream fileReader = null;
                try {
                    fileReader = new FileInputStream(f);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                byte[] cbuf = new byte[(int) f.length()];
                try {
                    fileReader.read(cbuf, 0, (int) f.length());
                    fileReader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                    continue;
                }
                byte[] base64EncodedImage = Base64.encodeBase64(cbuf);
                String result = new String(base64EncodedImage);
                result = "data:image/jpg;base64," + result;

                title = title.replace("_", " ").toLowerCase();
                title = title.replace("-", " ");
                Integer pr = prValues.get(Integer.parseInt(id));
                if (pr != null && pr != null && key != null) {
                    values = title.split(" ");
                    StringBuilder payload = new StringBuilder("{ \"img\":\"");
                    payload.append(result);
                    payload.append("\", \"href\":\"http://de.wikipedia.org/wiki/");
                    payload.append(key);
					payload.append("\"}");
					SuggestionField field = new SuggestionField(suggestionID++,
							key, Arrays.asList(title), payload.toString(), pr);
					fields.add(field);
                    if (fields.size() == maxNumberOfElements) {
                        return fields;
                    }
                }
            }
        } catch (FileNotFoundException e2) {
            // TODO Auto-generated catch block
            e2.printStackTrace();
        } catch (IOException e3) {
            // TODO Auto-generated catch block
            e3.printStackTrace();
        }

        return fields;
    }
}
