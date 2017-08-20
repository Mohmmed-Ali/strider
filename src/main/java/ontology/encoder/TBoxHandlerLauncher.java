package ontology.encoder;

import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;

public class TBoxHandlerLauncher {
    private final static String TEST_ONTO_1 = "univ-bench2.owl";

    public static void main(String[] args) {

        TBoxHandler tboxHandler = new TBoxHandler(TEST_ONTO_1, "lubm");
        tboxHandler.loadModel();
        tboxHandler.processConcepts(1L, 0, 1, OWL2.Thing.getURI());
        tboxHandler.getDictionary().normalizeConcepts();
        tboxHandler.getDictionary().displayConcepts();

        tboxHandler.processProperties(2L, 0, 2, OWL2.topDataProperty.getURI());
        tboxHandler.processProperties(3L, 0, 2, OWL2.topObjectProperty.getURI());
        tboxHandler.getDictionary().normalizeProperties();

        tboxHandler.getDictionary().addProperty(RDF.type.getURI(), 0, 0, 1);
        tboxHandler.getDictionary().addProperty(OWL2.sameAs.getURI(), 1, 0, 1);

//        tboxHandler.getDictionary().displayProperties();

        tboxHandler.getDictionary().save("conceptsURL2Id");
        tboxHandler.getDictionary().save("propertiesURL2Id");
    }

}
