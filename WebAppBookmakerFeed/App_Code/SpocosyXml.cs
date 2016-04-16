using System;
using System.Data;
using System.Web;
using System.Xml;
using System.IO;
using System.Collections.Generic;
using System.Text;
using MySql.Data;
using MySql.Data.MySqlClient;


namespace Spocosy
{

    public class SpocosyXml
    {
        //Holds the connection string to the database used.
        public string connectionString = "";

        //Holds message received back from class
        public string returnMessage = "VOID: ISPOSTBACK";

        //Vars used for output message
        int insertCount = 0;
        int updateCount = 0;

        //List that hold info on which nodes and attributes to use.
        //These lists are populated in populateLists() function
        private string nodeList = "";
        private Dictionary<string, string> attribList = new Dictionary<string, string>();

        //ID of XML file. This id is generated after push when xml file is saved.
        public int id = 0;

        //String that holds the xml data. Only used in push. When parsed myXml is used.
        public string xmlData = "";

        //XmlDocument var that is used for parsing
        public XmlDocument myXml = new XmlDocument();

        //Gets push data from POST var "xml" and saves the file to the filesystem
        public void getPushData()
        {

            //Get the xml data from the InputStream.For some reason HttpContext.Current.Request.Form["xml"] doesn't work
            var stream = HttpContext.Current.Request.InputStream;
            byte[] buffer = new byte[stream.Length];
            stream.Read(buffer, 0, buffer.Length);
            this.xmlData = Encoding.UTF8.GetString(buffer);

            //Extract the xml
            int first = this.xmlData.IndexOf("<?xml version=");
            int last = this.xmlData.IndexOf("</spocosy>") + "</spocosy>".Length;
            this.xmlData = this.xmlData.Substring(first, last - first);

            //this.xmlData = (HttpContext.Current.Request.Form["xml"] == null) ? "" : HttpContext.Current.Request.Form["xml"];

            if (xmlData == "")
            {
                //no data recieved
                this.returnMessage = "NO XML DATA RECIEVED";
                return;
            }
            else
            {
                //Validate XML data
                try
                {
                    this.myXml.Load(new StringReader(this.xmlData));
                }
                catch
                {
                    //Invalid XML Data
                    this.returnMessage = "ERROR: INVALID XML DATA";
                    //Save XML file in error folder
                    string nowDate = DateTime.Now.ToString("yyyy-MM-dd_HH_mm_ss");
                    TextWriter errWriter = new StreamWriter(HttpContext.Current.Server.MapPath(@"~\files\error\pushError_" + nowDate + ".xml"));
                    errWriter.Write(this.xmlData);
                    errWriter.Close();

                    return;
                }
                //In your database, log that you have received new xml data.
                MySqlConnection myConnection = new MySqlConnection(this.connectionString);
                MySqlCommand myCommand = new MySqlCommand("insert into saved_xml () values(); select LAST_INSERT_ID()", myConnection);
                myCommand.CommandType = CommandType.Text;
                myConnection.Open();
                //Holds the auto id that will be used as filename for the xml file.
                this.id = Convert.ToInt32(myCommand.ExecuteScalar());
                myCommand.Dispose();
                myConnection.Close();
                myConnection.Dispose();

                //Save the XML file to the filesystem
                TextWriter myWriter = new StreamWriter(HttpContext.Current.Server.MapPath(@"~\files\" + this.id + ".xml"));
                myWriter.Write(this.xmlData);
                myWriter.Close();

                // Verify that the file was saved correctly
                FileInfo filesize = new FileInfo(HttpContext.Current.Server.MapPath(@"~\files\" + this.id + ".xml"));
                if (filesize.Length <= 0)
                {
                    this.returnMessage = "ERROR: File not saved, size = 0 (files/" + this.id + ".xml)";
                    return;
                }
                //this.returnMessage = "SUCCESS: XML Saved (files/" + this.id + ".xml)";
                this.returnMessage = "RECEIVED_OK";

            }
        }

        //Load XML Data from file
        public void loadXML(int inid)
        {
            this.id = inid;
            if (!File.Exists(HttpContext.Current.Server.MapPath(@"~\files\" + this.id + ".xml")))
            {
                this.returnMessage = "ERROR: FILE NOT FOUND (files/" + this.id + ".xml)";
                xmlDone(false);
                return;
            }
            try
            {
                this.myXml.Load(HttpContext.Current.Server.MapPath(@"~\files\" + this.id + ".xml"));
            }
            catch
            {
                //Invalid XML Data
                this.returnMessage = "ERROR: INVALID XML DATA";
                return;
            }
        }

        //Parse and save the XML Data
        public void parseData()
        {
            populateLists();
            foreach (XmlNode node in this.myXml.ChildNodes)
            {
                nodeLoop(node, 0);
            }
            xmlDone(true);
        }

        //Loops through all nodes in the XML file.
        //This function loops itself to traverse through the XML tree.
        private void nodeLoop(XmlNode node, int lvl)
        {

            //Parse node if the name is contained in defined nodeList
            if (this.inList(this.nodeList, node.Name)) { parseNode(node); }

            //Loop through childNodes of current node
            foreach (XmlNode childNode in node.ChildNodes)
            {

                nodeLoop(childNode, lvl + 1);
            }
        }

        private void parseNode(XmlNode node)
        {

            //Hardcode to switch name "event_participant" to "event_participants"
            string nodeName = (node.Name == "event_participant") ? "event_participants" : node.Name;
            int n_xml = Convert.ToInt32(node.Attributes["n"].Value);
            int id_xml = Convert.ToInt32(node.Attributes["id"].Value);


            //check if node is already in the database and if it has changed
            MySqlConnection myConnection = new MySqlConnection(this.connectionString);
            MySqlCommand myCommand1 = new MySqlCommand("select IF(count(*)=0, -1, n) as n from " + nodeName + " where id=@id");
            MySqlCommand myCommand2 = new MySqlCommand();
            MySqlTransaction SQLtrans = default(MySqlTransaction);
            int num = 0;
            int num_del = 0;
            int i = 0;
            string msg = "";

            try
            {
                myCommand1.CommandType = CommandType.Text;
                myCommand1.Connection = myConnection;
                myCommand1.Parameters.Add(new MySqlParameter("id", id_xml));
                myCommand1.CommandTimeout = 300;
                myConnection.Open();

                //Must open connection before starting transaction.
                SQLtrans = myConnection.BeginTransaction();
                myCommand1.Transaction = SQLtrans;

                int n_db = Convert.ToInt32(myCommand1.ExecuteScalar());

                //Only save node if it does'nt exist or it has changed
                if (n_xml > n_db)
                {
                    //Holds values to be saved
                    Dictionary<string, string> values = new Dictionary<string, string>();

                    //String that holds fieldNames for query
                    StringBuilder queryFields = new StringBuilder();

                    //Tablename. Hardcode "event_participant" to "event_participants"
                    string tableName = (node.Name == "event_participant") ? "event_participants" : node.Name;

                    //Loop through attributes in node and check if attribute name exists in this.attribList
                    foreach (XmlAttribute attrib in node.Attributes)
                    {
                        if (inList(attribList[tableName], attrib.Name) || inList(attribList["ALL"], attrib.Name))
                        {
                            values.Add(attrib.Name, attrib.Value.ToString());
                            queryFields.Append(attrib.Name + ((n_db > -1) ? "=@" + attrib.Name : "") + ",");
                        }
                    }
                    //Remove last comma
                    queryFields.Length = queryFields.Length - 1;

                    myCommand2.CommandType = CommandType.Text;
                    myCommand2.Connection = myConnection;
                    myCommand2.CommandTimeout = 300;

                    if (n_db == -1)
                    {
                        myCommand2.CommandText = "INSERT INTO " + tableName + " (" + queryFields.ToString() + ") VALUES(@" + queryFields.Replace(",", ",@").ToString() + ")";
                        this.insertCount++;
                    }
                    else
                    {
                        myCommand2.CommandText = "UPDATE " + tableName + " SET " + queryFields.ToString() + " WHERE id=@id";
                        this.updateCount++;
                    }

                    //Add mysql parameters to query (values to be inserted/updated)
                    foreach (string key in values.Keys)
                    {
                        myCommand2.Parameters.Add(new MySqlParameter(key, values[key]));
                    }

                    myCommand2.ExecuteNonQuery();

                    //We are done. Now commit the transaction - actually change the DB.
                    SQLtrans.Commit();

                }
            }
            catch (Exception e)
            {
                //If anything went wrong attempt to rollback transaction
                try
                {
                    SQLtrans.Rollback();
                }
                catch { }

                HttpContext.Current.Response.Write(e.Message + "<br />");
                HttpContext.Current.Response.Write(myCommand2.CommandText + "<br />");
            }
            finally
            {
                try
                {
                    //Whatever happens, you will land here and attempt to close the connection.
                    myConnection.Close();
                }
                catch { }

            }

            this.returnMessage = "SUCCESS: " + this.insertCount + " inserted, " + this.updateCount + " updated from " + this.id + ".xml";
            

            }


        //Called when XML file is done parsing.
        private void xmlDone(bool isSuccess)
        {
            //Delete id from saved_xml (log table)
            MySqlConnection myConnection = new MySqlConnection(this.connectionString);
            MySqlCommand myCommand = new MySqlCommand("delete from saved_xml where id=@id");
            myCommand.CommandType = CommandType.Text;
            myCommand.Connection = myConnection;
            myCommand.Parameters.Add(new MySqlParameter("id", this.id));
            myConnection.Open();
            myCommand.ExecuteNonQuery();

            myConnection.Close();

            try
            {
                //Move the XML file. (if success to "parsed" else to "error")
                File.Move(HttpContext.Current.Server.MapPath(@"~\files\" + this.id + ".xml"), HttpContext.Current.Server.MapPath(@"~\files\" + ((isSuccess) ? "parsed" : "error") + @"\" + this.id + ".xml"));
            }
            catch { }
        }

        //Gets all new XML files into collection
        public List<SpocosyXml> newXml()
        {
            List<SpocosyXml> tempList = new List<SpocosyXml>();
            MySqlConnection myConnection = new MySqlConnection(this.connectionString);
            MySqlCommand myCommand = new MySqlCommand("Select id from saved_xml");
            myCommand.CommandType = CommandType.Text;
            myCommand.Connection = myConnection;

            myConnection.Open();
            MySqlDataReader myReader = myCommand.ExecuteReader();

            int loop = 0;
            while (myReader.Read())
            {
                loop++; // Add one
                SpocosyXml tmp = new SpocosyXml();
                tmp.connectionString = this.connectionString;
                tmp.loadXML(Convert.ToInt32(myReader["id"]));
                tempList.Add(tmp);
                // For memory purposes
                if (loop == 2)
                {
                    break;
                }
            }

            myConnection.Close();
            myReader.Dispose();
            myCommand.Dispose();

            return tempList;
        }

        //Populate lists
        private void populateLists()
        {
            //Only these nodes should be parsed
            // Original this.nodeList = "event_participant,country,status_desc,result_type,incident_type,event_incident_type,event_incident_type_text,lineup_type,offence_type,standing_type,standing_type_param,standing_config,language_type,sport,participant,tournament_template,tournament,tournament_stage,event,event_participants,outcome,bettingoffer,object_participants,lineup,incident,event_incident,event_incident_detail,result,standing,standing_participants,standing_data,property,language,image,reference,reference_type,odds_provider,scope_type,scope_data_type,event_scope,event_scope_detail,scope_result,lineup_scope_result,venue_data,venue_data_type,venue";
            this.nodeList = "event_participant,country,status_desc,result_type,incident_type,event_incident_type,event_incident_type_text,lineup_type,offence_type,standing_type,standing_type_param,standing_config,language_type,sport,participant,tournament_template,tournament,tournament_stage,event,event_participants,outcome,bettingoffer,property,language,image,reference,reference_type,odds_provider,scope_type,scope_data_type,event_scope,event_scope_detail,scope_result,lineup_scope_result,venue_data,venue_data_type,venue,venue_type";

            //Attributes that should always be included
            this.attribList.Add("ALL", "id,n,ut,del");

            /*
             * List of tables, which each contain a list of attributes, remove or add
             * attributes here to have it inculded in the database 
             * (make sure it the field exists in the database when adding attributes)
             */
            this.attribList.Add("bettingoffer", "outcomeFK,odds_providerFK,odds,odds_old,active,is_back,is_single,is_live,volume,currency,couponKey");
            this.attribList.Add("country", "name");
            this.attribList.Add("event", "name,tournament_stageFK,startdate,eventstatusFK,status_type,status_descFK");
            this.attribList.Add("event_incident", "eventFK,sportFK,event_incident_typeFK,elapsed,elapsed_plus,comment,sortorder");
            this.attribList.Add("event_incident_detail", "type,event_incidentFK,participantFK,value");
            this.attribList.Add("event_incident_type", "player1,player2,team,comment,subtype1,subtype2,name,type,comment_type,player2_type");
            this.attribList.Add("event_incident_type_text", "event_incident_typeFK,name");
            this.attribList.Add("event_participants", "number,participantFK,eventFK");
            this.attribList.Add("image", "object,objectFK,type,contenttype,name,value");
            this.attribList.Add("incident", "event_participantsFK,incident_typeFK,incident_code,elapsed,sortorder,ref_participantFK");
            this.attribList.Add("incident_type", "name,subtype");
            this.attribList.Add("language", "object,objectFK,language_typeFK,name");
            this.attribList.Add("language_type", "name,description");
            this.attribList.Add("lineup", "event_participantsFK,participantFK,lineup_typeFK,shirt_number,pos");
            this.attribList.Add("lineup_type", "name");
            this.attribList.Add("object_participants", "object,objectFK,participantFK,participant_type,active");
            this.attribList.Add("offence_type", "name");
            this.attribList.Add("odds_provider", "name,url,bookmaker,preferred,betex,active");
            this.attribList.Add("outcome", "object,objectFK,type,event_participant_number,scope,subtype,iparam,iparam2,dparam,dparam2,sparam");
            this.attribList.Add("participant", "name,gender,type,countryFK,enetID,enetSportID");
            this.attribList.Add("property", "object,objectFK,type,name,value");
            this.attribList.Add("reference", "object,objectFK,refers_to,name");
            this.attribList.Add("reference_type", "name,description");
            this.attribList.Add("result", "event_participantsFK,result_typeFK,result_code,value");
            this.attribList.Add("result_type", "name,code");
            this.attribList.Add("sport", "name");
            this.attribList.Add("standing", "object,objectFK,standing_typeFK,name");
            this.attribList.Add("standing_config", "standingFK,standing_type_paramFK,value,sub_param");
            this.attribList.Add("standing_data", "standing_participantsFK,standing_type_paramFK,value,code,sub_param");
            this.attribList.Add("standing_participants", "standingFK,participantFK,rank");
            this.attribList.Add("standing_type", "name,description");
            this.attribList.Add("standing_type_param", "standing_typeFK,code,name,type,value");
            this.attribList.Add("status_desc", "name,status_type");
            this.attribList.Add("tournament", "name,tournament_templateFK");
            this.attribList.Add("tournament_stage", "name,tournamentFK,gender,countryFK,startdate,enddate");
            this.attribList.Add("tournament_template", "name,sportFK,gender");
            this.attribList.Add("scope_type", "name,description");
            this.attribList.Add("scope_data_type", "name,description");
            this.attribList.Add("event_scope", "eventFK,scope_typeFK");
            this.attribList.Add("event_scope_detail", "event_scopeFK,name,value");
            this.attribList.Add("scope_result", "event_participantsFK,event_scopeFK,scope_data_typeFK,value");
            this.attribList.Add("lineup_scope_result", "lineupFK,event_scopeFK,scope_data_typeFK,value");
            this.attribList.Add("venue_data", "value,venue_data_typeFK,venueFK");
            this.attribList.Add("venue_data_type", "name");
            this.attribList.Add("venue", "name,countryFK,venue_typeFK");
            this.attribList.Add("venue_type", "name");
        }


        //Helper functions
        //Check if value is in comma seperated list
        private bool inList(string list, string checkString)
        {
            return (list.StartsWith(checkString) || list.IndexOf("," + checkString) != -1);
        }
    }
}
