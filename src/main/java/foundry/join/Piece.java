package foundry.join;

/** One record on its way through the join: a packet or an enrichment, keyed by the packet's guid. */
public class Piece {
    public String guid;
    public String schema;
    public String json;

    public Piece() {}

    public Piece(String guid, String schema, String json) {
        this.guid = guid;
        this.schema = schema;
        this.json = json;
    }
}
