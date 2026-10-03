package foundry.join;

/** One record on its way through the join: a packet or an enrichment, keyed by the packet's guid. */
public class Piece {
    public String guid;
    public String type;
    public String json;

    public Piece() {}

    public Piece(String guid, String type, String json) {
        this.guid = guid;
        this.type = type;
        this.json = json;
    }
}
