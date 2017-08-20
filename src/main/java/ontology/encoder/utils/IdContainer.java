package ontology.encoder.utils;

public class IdContainer {
    private long id;
    private int encodingStart, localLength;

    public IdContainer(long id, int encodingStart, int localLength) {
        this.id = id;
        this.encodingStart = encodingStart;
        this.localLength = localLength;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public int getEncodingStart() {
        return encodingStart;
    }

    public int getLocalLength() {
        return localLength;
    }

    public String prettyPrint() {
        return "id = " + id + ", localLength =" + localLength + ", encodingStart =" + encodingStart;
    }
}
