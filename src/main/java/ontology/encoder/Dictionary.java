package ontology.encoder;


import ontology.encoder.utils.IdContainer;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


class Dictionary {
    private static final String EXT = "dct";
    private static Dictionary instance = null;
    private String filename;
    private InputStream in;
    private Map<Long, String> conceptsId2URL = new HashMap<Long, String>();
    private Map<String, IdContainer> conceptsURL2Id = new HashMap<String, IdContainer>();
    private Map<Long, String> propertiesId2URL = new HashMap<Long, String>();
    private Map<String, IdContainer> propertiesURL2Id = new HashMap<String, IdContainer>();

    private Dictionary(String filename) {
        this.filename = filename;
    }

    static Dictionary getInstance(String filename) {
        if (instance == null)
            instance = new Dictionary(filename);
        return instance;
    }

    void addConceptURL2IdItem(String concept, long id, int localLength, int encodingStart) {
        conceptsURL2Id.put(concept, new IdContainer(id, encodingStart, localLength));
    }

    void addPropertyURL2IdItem(String property, long id, int localLength, int encodingStart) {
        propertiesURL2Id.put(property, new IdContainer(id, encodingStart, localLength));
    }

    void displayConcepts() {
        Set<String> idSet = conceptsURL2Id.keySet();
        for (String concept : idSet) {
            System.out.println(concept + "  => " + conceptsURL2Id.get(concept).prettyPrint());
        }
    }

    void displayProperties() {
        Set<String> idSet = propertiesURL2Id.keySet();
        for (String property : idSet) {
            System.out.println(property + "  => " + propertiesURL2Id.get(property).prettyPrint());
        }
    }

    private int getMaxId(Map<String, IdContainer> itemsURL2Id, Set<String> idSet) {
        Long max = 0L;
        for (String item : idSet) {
            if (max < itemsURL2Id.get(item).getId())
                max = itemsURL2Id.get(item).getId();
        }
        return (int) Math.ceil(Math.log(max) / Math.log(2));

    }

    private int getItemEncodingLength(IdContainer idContainer) {
        if (idContainer.getEncodingStart() == 0)
            return idContainer.getLocalLength();
        else
            return (int) Math.ceil(Math.log(idContainer.getId()) / Math.log(2));
    }

    void normalizeConcepts() {
        Set<String> idSet = conceptsURL2Id.keySet();
        int encodingLength = getMaxId(conceptsURL2Id, idSet);

        for (String concept : idSet) {
            IdContainer tmpIdContainer = conceptsURL2Id.get(concept);
            int conceptLength = getItemEncodingLength(tmpIdContainer);
            long tmpId = tmpIdContainer.getId() << (encodingLength - conceptLength);
            tmpIdContainer.setId(tmpId);
            conceptsId2URL.put(tmpId, concept);
        }
    }

    void normalizeProperties() {
        Set<String> idSet = propertiesURL2Id.keySet();
        int encodingLength = getMaxId(propertiesURL2Id, idSet);

        for (String concept : idSet) {
            IdContainer tmpIdContainer = propertiesURL2Id.get(concept);
            int propertyLength = getItemEncodingLength(tmpIdContainer);
            long tmpId = tmpIdContainer.getId() << (encodingLength - propertyLength);
            tmpIdContainer.setId(tmpId);
            propertiesId2URL.put(tmpId, concept);
        }
    }

    void addProperty(String property, long id, int encodingStart, int localLength) {
        propertiesURL2Id.put(property, new IdContainer(id, encodingStart, localLength));
        propertiesId2URL.put(id, property);
    }

    void save(String target) {
        File file = new File(filename + "_" + target + "." + EXT);
        try {
            FileOutputStream f = new FileOutputStream(file);
            PrintStream ps = new PrintStream(f);
            if (Objects.equals(target, "conceptsURL2Id")) {
                printConceptKVPair(ps, conceptsURL2Id);
            } else if (Objects.equals(target, "propertiesURL2Id")) {
                printPropertyKVPair(ps, propertiesURL2Id);
            }
            ps.close();
        } catch (IOException ioe) {
            System.err.println("Fail to open file " + target);
        }
    }

    private void prettyPrintURL2Id(PrintStream ps, Map<String, IdContainer> map) {
        Set<String> idSet = map.keySet();
        int encodingLength = getMaxId(map, idSet);
        for (String key : idSet) {
            int shift = encodingLength - (map.get(key).getEncodingStart() + map.get(key).getLocalLength());
            long prefix = map.get(key).getId() >> shift;
            long upperBound = (prefix + 1) << shift;
            ps.println(key + " E" +
                    map.get(key).getId() + " " +
                    map.get(key).getId() + " " +
                    upperBound);
        }
    }

    private void printConceptKVPair(PrintStream ps, Map<String, IdContainer> map) {
        Set<String> idSet = map.keySet();
        int encodingLength = getMaxId(map, idSet);
        for (String key : idSet) {
            int shift = encodingLength - (map.get(key).getEncodingStart() + map.get(key).getLocalLength());
            long prefix = map.get(key).getId() >> shift;
            long upperBound = (prefix + 1) << shift;
            ps.println(key + " C" +
                    map.get(key).getId() + " " +
                    map.get(key).getId() + " " +
                    upperBound);
        }
    }

    private void printPropertyKVPair(PrintStream ps, Map<String, IdContainer> map) {
        Set<String> idSet = map.keySet();
        int encodingLength = getMaxId(map, idSet);
        for (String key : idSet) {
            int shift = encodingLength - (map.get(key).getEncodingStart() + map.get(key).getLocalLength());
            long prefix = map.get(key).getId() >> shift;
            long upperBound = (prefix + 1) << shift;
            ps.println(key + " P" +
                    map.get(key).getId() + " " +
                    map.get(key).getId() + " " +
                    upperBound);
        }
    }

}
