/*
 * Copyright 2015 col.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package psidev.psi.mi.jami.examples.interactionviewer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author col
 */
public class Indexer {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Indexer.class.getName()).log(Level.SEVERE, null, ex);
        }
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        String url = "jdbc:postgresql://localhost/intactJson";
        String user = "";
        String password = "";

        try {
            con = DriverManager.getConnection(url, user, password);
            st = con.createStatement();

            String statement = "SELECT * FROM filename_json;";
            System.out.println(statement);
            rs = st.executeQuery(statement);
 
            Pattern idRegEx = Pattern.compile("\"uniprotkb_(.*?)(\"|(-PRO))");

            while (rs.next()) {
//            rs.next();
                String fileName = rs.getString("filename");
                String json = rs.getString("json");
                System.out.println("F:" + fileName);
                Matcher matcher = idRegEx.matcher(json);

                Set accSet = new HashSet();

                while (matcher.find()) {
                    accSet.add(matcher.group(1));
                }

                System.out.println(accSet);

                Iterator<String> accs = accSet.iterator();
                while (accs.hasNext()) {
                    String acc = accs.next();

                    try {
                        con = DriverManager.getConnection(url, user, password);
                        st = con.createStatement();

                        String statement2 = "INSERT INTO uniprotkb_filename (uniprotkb, filename) VALUES ('"
                                + acc + "','" + fileName + "');";
                        System.out.println(statement2);
                        st.execute(statement2);

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

                }

            }
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
