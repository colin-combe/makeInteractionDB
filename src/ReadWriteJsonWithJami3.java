package psidev.psi.mi.jami.examples.interactionviewer;

import psidev.psi.mi.jami.commons.MIDataSourceOptionFactory;
import psidev.psi.mi.jami.commons.PsiJami;
import psidev.psi.mi.jami.datasource.InteractionStream;
import psidev.psi.mi.jami.datasource.InteractionWriter;
import psidev.psi.mi.jami.factory.InteractionWriterFactory;
import psidev.psi.mi.jami.factory.MIDataSourceFactory;
import psidev.psi.mi.jami.json.InteractionViewerJson;
import psidev.psi.mi.jami.json.MIJsonOptionFactory;
import psidev.psi.mi.jami.model.Interaction;
import psidev.psi.mi.jami.model.InteractionEvidence;
import psidev.psi.mi.jami.model.ModelledInteraction;

import java.io.File;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FilenameUtils;

/**
 * This class shows how to read MITAB/PSI-MI XML (and other sources) and how to
 * write JAMI objects in MITAB/PSI-MI XML (or other JAMI interaction writers)
 *
 * @author Marine Dumousseau (marine@ebi.ac.uk)
 * @version $Id$
 * @since <pre>27/06/14</pre>
 */
public class ReadWriteJsonWithJami {

    public static void main(String[] args) throws Exception {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger("YO LOGGER").log(Level.SEVERE, null, ex);
        }
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        String url = "jdbc:postgresql://localhost/intactJson";
        String user = "";
        String password = "";

        String path = "/pmid";
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();
        Arrays.sort(listOfFiles);

        // initialise default factories for reading and writing MITAB/PSI-MI XML files
        PsiJami.initialiseAllMIDataSources();
        // initialise all json writers
        InteractionViewerJson.initialiseAllMIJsonWriters();

        // the option factory for reading files and other datasources
        MIDataSourceOptionFactory optionfactory = MIDataSourceOptionFactory.getInstance();
        // the datasource factory for reading MITAB/PSI-MI XML files and other datasources
        MIDataSourceFactory dataSourceFactory = MIDataSourceFactory.getInstance();

        for (int i = 0; i < listOfFiles.length; i++) {
            System.out.println(i);
                
            File item = listOfFiles[i];

            // get default options for a file. It will identify if the file is MITAB or PSI-MI XML file and then it will load the appropriate options.
            // By default, the datasource will be streaming (only returns an iterator of interactions), and returns a source of Interaction objects.
            // The default options can be overridden using the optionfactory or by manually adding options listed in MitabDataSourceOptions or PsiXmlDataSourceOptions
            Map<String, Object> parsingOptions = optionfactory.getDefaultOptions(item);

            InteractionStream interactionSource = null;
            InteractionWriter jsonInteractionWriter = null;
            StringWriter myWriter = new StringWriter();
            try {
                // Get the stream of interactions knowing the default options for this file
                interactionSource = dataSourceFactory.
                        getInteractionSourceWith(parsingOptions);

                // writing MITAB and PSI-XML files
                // the option factory for writing in json
                MIJsonOptionFactory optionwriterFactory = MIJsonOptionFactory.getInstance();
                // the interaction writer factory for writing MI files. Other writers can be dynamically added to the interactionWriterFactory
                InteractionWriterFactory writerFactory = InteractionWriterFactory.getInstance();

                // get default options for writing Json file.
                // By default, the writer will be a n-ary json writer             
                Map<String, Object> jsonWritingOptions = optionwriterFactory.getDefaultJsonOptions(myWriter);

                // Get the default Json writer
                jsonInteractionWriter = writerFactory.getInteractionWriterWith(jsonWritingOptions);

                // parse the stream and write as we parse
                // the interactionSource can be null if the file is not recognized or the provided options are not matching any existing/registered datasources
                if (interactionSource != null) {
                    Iterator interactionIterator = interactionSource.getInteractionsIterator();

                    // start the writers (write headers, etc.)
                    jsonInteractionWriter.start();

                    while (interactionIterator.hasNext()) {
                        Interaction interaction = (Interaction) interactionIterator.next();

                        // most of the interactions will have experimental data attached to them so they will be of type InteractionEvidence
                        if (interaction instanceof InteractionEvidence) {
                            InteractionEvidence interactionEvidence = (InteractionEvidence) interaction;
                            // process the interaction evidence
                        } // modelled interactions are equivalent to abstractInteractions in PSI-MI XML 3.0. They are returned when the interaction is not an
                        // experimental interaction but a 'modelled' one extracted from any experimental context
                        else if (interaction instanceof ModelledInteraction) {
                            ModelledInteraction modelledInteraction = (ModelledInteraction) interaction;
                            // process the modelled interaction
                        }

                        // write the interaction in Json
                        jsonInteractionWriter.write(interaction);
                    }

                    // end the writers (write end tags, etc.)
                    jsonInteractionWriter.end();
                }
            } finally {
                // always close the opened interaction stream
                if (interactionSource != null) {
                    interactionSource.close();
                }
                // always close the opened interaction writers
                if (jsonInteractionWriter != null) {
                    jsonInteractionWriter.close();
                }
            }

            String filename = FilenameUtils.getName(item.getName());
            //System.out.println(myWriter.toString());
            String jsonData = myWriter.toString();
            jsonData = jsonData.replaceAll("'", "''");

            try {
                con = DriverManager.getConnection(url, user, password);
                st = con.createStatement();

                String statement = "INSERT INTO filename_json (filename, json) VALUES ('"
                        + filename + "','" + jsonData + "');";
//                System.out.println(statement);
                st.execute(statement);

            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger("yo");//Version.class.getName());
                lgr.log(Level.SEVERE, ex.getMessage(), ex);

            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
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
        }
    }
}
