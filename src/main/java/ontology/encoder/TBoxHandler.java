package ontology.encoder;

import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.util.FileManager;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.OWL2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;

class TBoxHandler {
    private static final Logger log = LoggerFactory.getLogger(TBoxHandler.class);
    private String tbox;
    private Dictionary dictionary;
    private OntModel ontModel;

    TBoxHandler(String tbox, String dictionaryName) {
        this.tbox = tbox;
        this.dictionary = Dictionary.getInstance(dictionaryName);
    }

    void loadModel() {
        ontModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF);  //PelletReasonerFactory.THE_SPEC );  //
        InputStream in = FileManager.get().open(tbox);
        if (in == null) {
            throw new IllegalArgumentException("File: " + tbox + " not found");
        }
        ontModel.read(in, null);
    }

    private boolean isOWLProperty(OntProperty property) {
        return "http://www.w3".equals(property.getURI().subSequence(0, 13));
    }

    private boolean hasSuperProperty(OntProperty p) {
        ExtendedIterator<? extends OntProperty> it = p.listSuperProperties();
        int occur = 0;
        while (it.hasNext()) {
            occur++;
            break;
        }
        return occur != 0;
    }

    private int countConcreteSubClasses(OntClass cls) {
        int count = 0;
        ExtendedIterator<OntClass> it = cls.listSubClasses(true);
        while (it.hasNext()) {
            OntClass tmpCls = it.next();
            if (validSubConcept(tmpCls, cls)) {
                count++;
            }
        }
        return count;
    }

    private boolean validSubConcept(OntClass subCls, OntClass cls) {
        return !subCls.isAnon() && !subCls.equals(OWL2.Nothing) && !subCls.equals(cls);
    }

    private ArrayList<OntProperty> getSubProperties(OntProperty property) {
        ArrayList<OntProperty> subProperties = new ArrayList<>();
        if (property.equals(OWL2.topDataProperty)) {
            ExtendedIterator<DatatypeProperty> it = ontModel.listDatatypeProperties();
            while (it.hasNext()) {
                OntProperty tmpPrp = it.next();
                if (!hasSuperProperty(tmpPrp))
                    subProperties.add(tmpPrp);
            }
        } else if (property.equals(OWL2.topObjectProperty)) {
            ExtendedIterator<ObjectProperty> it = ontModel.listObjectProperties();
            while (it.hasNext()) {
                OntProperty tmpPrp = it.next();
                if (!hasSuperProperty(tmpPrp) && !isOWLProperty(tmpPrp))
                    subProperties.add(tmpPrp);
            }
        }
        ExtendedIterator<? extends OntProperty> it = property.listSubProperties(true);
        if (it != null) {
            while (it.hasNext()) {
                OntProperty tmpPrp = it.next();
                if (!isOWLProperty(tmpPrp))
                    if ((property == null && !hasSuperProperty(tmpPrp)) || (property != null)) {
                        subProperties.add(tmpPrp);
                    }
            }
        }
        return subProperties;
    }

    void processConcepts(long superConceptId, int encodingStart, int localLength, String concept) {
        OntClass cls = ontModel.createClass(concept);
        dictionary.addConceptURL2IdItem(cls.getURI(), superConceptId, localLength, encodingStart);
        int count = countConcreteSubClasses(cls);
        if (count > 0) {
            int nbBits = (int) Math.ceil(Math.log(count + 1) / Math.log(2));
            ExtendedIterator<OntClass> it = cls.listSubClasses(true);
            Long conceptId = (superConceptId << nbBits);
            while (it.hasNext()) {
                OntClass tmpCls = it.next();
                if (validSubConcept(tmpCls, cls)) {
                    long tmp = ++conceptId;
                    processConcepts(tmp, encodingStart + localLength, nbBits, tmpCls.getURI());
                }
            }
        }
    }

    void processProperties(long superPropertyId, int encodingStart, int localLength, String property) {
        OntProperty prop = ontModel.createOntProperty(property);
        dictionary.addPropertyURL2IdItem(prop.getURI(), superPropertyId, localLength, encodingStart);
        ArrayList<OntProperty> subProperties = getSubProperties(prop);
        if (subProperties.size() > 0) {
            int nbBits = (int) Math.ceil(Math.log(subProperties.size() + 1) / Math.log(2));
            Long propertytId = (superPropertyId << nbBits);
            for (OntProperty tmpProp : subProperties) {
                if ((property == null && !hasSuperProperty(tmpProp)) || (property != null)) {
                    long tmp = ++propertytId;
                    processProperties(tmp, encodingStart + localLength, nbBits, tmpProp.toString());
                }
            }
        }
    }

    Dictionary getDictionary() {
        return dictionary;
    }
}
