package uniprotparser;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class ReadXML {

    public static void main(String[] args) {
        /*
        DB connection
         */
 
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger("YO LOGGER").log(Level.SEVERE, null, ex);
        }
        Connection con = null;
        Statement st = null;
        String url = "jdbc:postgresql://localhost/interaction";

        try {

            Properties props = new Properties();
            props.setProperty("user", "col");
            con = DriverManager.getConnection(url, props);

            st = con.createStatement();
            String statement = "DROP TABLE IF EXISTS uniprot";
            st.execute(statement);
            statement = "CREATE TABLE uniprot (accession varchar(10) NOT NULL, json JSON NOT null);";
            st.execute(statement);
        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger("yo");//Version.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);

        } finally {
            try {
                if (st != null) {
                    st.close();
                }
                if (con != null) {
                    con.close();
                }

            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger("yo");
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
         
 /*
        SAX parser
         */
        SAXParserFactory spf = SAXParserFactory.newInstance();
        UniprotHandler handler = new UniprotHandler();
        try {
            SAXParser sp = spf.newSAXParser();
            sp.parse("/home/col/data/uniprot/uniprot_sprot.xml/data", handler);
        } catch (SAXException se) {
            se.printStackTrace();
        } catch (ParserConfigurationException pce) {
            pce.printStackTrace();
        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }
}

class UniprotHandler extends DefaultHandler {

    private static Connection connection;
    private static Statement statement;

    private static final String TAG_UNIPROT = "UNIPROT";
    private static final String TAG_ENTRY = "ENTRY";
    private static final String TAG_ACC = "ACCESSION";
    private static final String TAG_NAME = "NAME";
    private static final String TAG_RECOMMENDEDNAME = "RECOMMENDEDNAME";
    private static final String TAG_FULLNAME = "FULLNAME";
    private static final String TAG_GENE = "GENE";
    private static final String TAG_ORGANISM = "ORGANISM";
    private static final String TAG_COMMENT = "COMMENT";
    private static final String TAG_TEXT = "TEXT";
    private static final String TAG_LOCATION = "LOCATION";
    private static final String TAG_SUBCELLULARLOCATION = "SUBCELLULARLOCATION";
    private static final String TAG_KEYWORD = "KEYWORD";

    private static final String TAG_FEATURE = "FEATURE";
    private static final String TAG_POSITION = "POSITION";
    private static final String TAG_BEGIN = "BEGIN";
    private static final String TAG_END = "END";

    private static final String TAG_SEQUENCE = "SEQUENCE";
    //dbReferences
    //references

    private final Stack<String> tagsStack = new Stack<String>();
    private final StringBuilder tempVal = new StringBuilder();

    private JsonObjectBuilder jsonBuilder;
    private JsonObjectBuilder jsonFeatureBuilder;
    private Attributes currentElementAttributes;
    private boolean primaryAccessionSet;
    private String primaryAccession;
    private List<String> secondaryAccessions;
    private String[] organism; // array cells will hold common / scientific / synomyn
    private List<String> keywords;
    private List<String> comments;
    private List<String> locations;
    private JsonArrayBuilder features;

    private static final Map<String, String> featureTypeToSubsection = new HashMap<String, String>();;

    static {
        featureTypeToSubsection.put("SIGNAL PEPTIDE", "MOLECULE_PROCESSING");
        featureTypeToSubsection.put("CHAIN", "MOLECULE_PROCESSING");
        featureTypeToSubsection.put("LIPID MOIETY-BINDING REGION", "PTM");
        featureTypeToSubsection.put("INITIATOR METHIONINE", "MOLECULE_PROCESSING");
        featureTypeToSubsection.put("TOPOLOGICAL DOMAIN", "DOMAINS_AND_SITES");
        featureTypeToSubsection.put("NUCLEOTIDE PHOSPHATE-BINDING REGION", "DOMAINS_AND_SITES");
        featureTypeToSubsection.put("REGION OF INTEREST", "DOMAINS_AND_SITES");
        featureTypeToSubsection.put("SEQUENCE CONFLICT", "SEQUENCE_INFORMATION");
        featureTypeToSubsection.put("BINDING SITE", "DOMAINS_AND_SITES");
        featureTypeToSubsection.put("ACTIVE SITE", "DOMAINS_AND_SITES");
        featureTypeToSubsection.put("DISULFIDE BOND", "PTM");
        featureTypeToSubsection.put("TRANSMEMBRANE REGION", "TOPOLOGY");
        featureTypeToSubsection.put("COILED-COIL REGION", "DOMAINS_AND_SITES");
        featureTypeToSubsection.put("TOPO_DOM", "TOPOLOGY");
        featureTypeToSubsection.put("METAL ION-BINDING SITE", "DOMAINS_AND_SITES");
        featureTypeToSubsection.put("SHORT SEQUENCE MOTIF", "DOMAINS_AND_SITES");
        featureTypeToSubsection.put("STRAND", "STRUCTURAL");
        featureTypeToSubsection.put("HELIX", "STRUCTURAL");
        featureTypeToSubsection.put("TURN", "STRUCTURAL");
        featureTypeToSubsection.put("MODIFIED RESIDUE", "PTM");
        featureTypeToSubsection.put("REPEAT", "DOMAINS_AND_SITES");
        featureTypeToSubsection.put("SITE", "DOMAINS_AND_SITES");
        featureTypeToSubsection.put("ZINC FINGER REGION", "DOMAINS_AND_SITES");
        featureTypeToSubsection.put("COMPOSITIONALLY BIASED REGION", "SEQUENCE_INFORMATION");
        featureTypeToSubsection.put("DNA-BINDING REGION", "DOMAINS_AND_SITES");
        featureTypeToSubsection.put("SEQUENCE VARIANT", "VARIANTS");
        featureTypeToSubsection.put("MUTAGENESIS SITE", "MUTAGENESIS");
    }

    public void startElement(String uri, String localName, String qName, Attributes attributes) {
        pushTag(qName);
        tempVal.setLength(0);
        if (TAG_ENTRY.equalsIgnoreCase(qName)) {
            jsonBuilder = Json.createObjectBuilder();
            currentElementAttributes = attributes;
            primaryAccessionSet = false;
            secondaryAccessions = new ArrayList<String>();
            organism = new String[2];
            keywords = new ArrayList<String>();
            comments = new ArrayList<String>();
            locations = new ArrayList<String>();
            features = Json.createArrayBuilder();
        } else if (TAG_FEATURE.equalsIgnoreCase(qName)) {
            jsonFeatureBuilder = Json.createObjectBuilder();
            String type = attributes.getValue("type");
            jsonFeatureBuilder.add("type", type);
            String description = attributes.getValue("description");
            if (description != null) {
                jsonFeatureBuilder.add("description", description);
            }
            String category = featureTypeToSubsection.get(type.toUpperCase());
            if (category != null) {
                jsonFeatureBuilder.add("category", category);
            }
        }
    }

    public void characters(char ch[], int start, int length) {
        tempVal.append(ch, start, length);
    }

    public void endElement(String uri, String localName, String qName) {
        String tag = peekTag();
        popTag();
        String parentTag = peekTag();
        String value = tempVal.toString().trim();
        try {
            if (!qName.equals(tag)) {
                throw new InternalError();
            }

            if (TAG_ACC.equalsIgnoreCase(tag)) {
                if (!primaryAccessionSet) {
                    jsonBuilder.add("accession", value);
                    primaryAccessionSet = true;
                    primaryAccession = value;
                } else {
                    secondaryAccessions.add(value);
                }
            } else if (TAG_NAME.equalsIgnoreCase(tag)) {
                if (TAG_ENTRY.equalsIgnoreCase(parentTag)) {
                    jsonBuilder.add("name", value);
                } else if (TAG_GENE.equalsIgnoreCase(parentTag)) {
                    jsonBuilder.add("gene", value);
                } else if (TAG_ORGANISM.equalsIgnoreCase(parentTag)) {
                    String type = currentElementAttributes.getValue("type");
                    if (type.equalsIgnoreCase("common")) {
                        organism[0] = value;
                    } else if (type.equalsIgnoreCase("scientific")) {
                        organism[1] = value;
                    } else if (type.equalsIgnoreCase("synonym")) {
                        //organism[2] = value;
                    }
                }
            } else if (TAG_FULLNAME.equalsIgnoreCase(tag)
                    && TAG_RECOMMENDEDNAME.equalsIgnoreCase(parentTag)) {
                jsonBuilder.add("fullName", value);
            } else if (TAG_KEYWORD.equalsIgnoreCase(tag)) {
                keywords.add(value);
            } else if (TAG_TEXT.equalsIgnoreCase(tag)) {
                if (TAG_COMMENT.equalsIgnoreCase(parentTag)) {
                    comments.add(value);
                }
            } else if (TAG_LOCATION.equalsIgnoreCase(tag)) {
                if (TAG_SUBCELLULARLOCATION.equalsIgnoreCase(parentTag)) {
                    locations.add(value);
                }
            } else if (TAG_SEQUENCE.equalsIgnoreCase(tag)) {
                jsonBuilder.add("sequence", value);
            } else if (TAG_FEATURE.equalsIgnoreCase(tag)) {
                features.add(jsonFeatureBuilder);
            } else if (TAG_POSITION.equalsIgnoreCase(tag)
                    || TAG_BEGIN.equalsIgnoreCase(tag)
                    || TAG_END.equalsIgnoreCase(tag)) {
                if (TAG_LOCATION.equalsIgnoreCase(parentTag)) {
                    String pos = currentElementAttributes.getValue("position");
                    if (TAG_POSITION.equalsIgnoreCase(tag)) {
                        jsonFeatureBuilder.add("begin", pos);
                        jsonFeatureBuilder.add("end", pos);
                    } else {
                        //System.out.println(tag + "\t" + pos);
                        if (pos == null) {
                            pos = "?";
                        }
                        jsonFeatureBuilder.add(tag, pos);
                    }
                }
            } else if (TAG_ENTRY.equalsIgnoreCase(tag)) {
                //end
                jsonBuilder.add("organism", String.join(" / ", Arrays.asList(organism)));
                jsonBuilder.add("keywords", String.join(", ", keywords));
                jsonBuilder.add("comments", String.join(", ", comments));
                jsonBuilder.add("locations", String.join(", ", locations));
                jsonBuilder.add("secondaryAccessions", String.join(", ", secondaryAccessions));
                jsonBuilder.add("features", features);
                JsonObject json = jsonBuilder.build();

                String jsonString = json.toString();
                jsonString = jsonString.replace("'", "''");

                      
                String insert = "INSERT INTO uniprot VALUES ('" + primaryAccession
                        + "','" + jsonString + "')";
                                
                statement.addBatch(insert);

                for (String secondaryAccession : secondaryAccessions) {
                    insert = "INSERT INTO uniprot VALUES ('" + secondaryAccession
                            + "','" + jsonString + "')";
                    statement.addBatch(insert);
                }

                statement.executeBatch();
                statement.clearBatch();
                 
//                System.out.println(primaryAccession + ">\t" + String.join(", ", locations));
//            System.out.println(json.toString());
                //System.out.println(secondaryAccessions.toString());
            }
        } catch (Exception e) {
            System.out.println(primaryAccession + "\t" + tag + "\t>" + value + "<");
            e.printStackTrace();
            System.exit(1);

        }
    }

    @Override
    public void startDocument() {
        String url = "jdbc:postgresql://localhost/interaction";
        
        try {

            Properties props = new Properties();
            props.setProperty("user", "col");
            connection = DriverManager.getConnection(url, props);
            connection.setAutoCommit(false);
            statement = connection.createStatement();
                
        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger("yo");//Version.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);

        }
         
        pushTag("");
    }

    @Override
    public void endDocument() {
        try {
            connection.commit();
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger("yo");
            lgr.log(Level.WARNING, ex.getMessage(), ex);
        }
        System.out.println("done here");
    }

    private void pushTag(String tag) {
        tagsStack.push(tag);
    }

    private String popTag() {
        return tagsStack.pop();
    }

    private String peekTag() {
        return tagsStack.peek();
    }
}
