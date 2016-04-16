<%@ Page Language="C#" ValidateRequest="false" %>
<%@ Import Namespace="Spocosy" %>
<script runat="server">
    protected void Page_Load(object sender, EventArgs e)
    {
        HttpContext.Current.Server.ScriptTimeout = 1800; // 10 minutes
        SpocosyXml xmlData = new SpocosyXml();
        xmlData.connectionString = @"Database=OddsMatching;Data Source=eu-cdbr-azure-north-e.cloudapp.net;User Id=b083cb50265fec;Password=263fb5f7;Connect Timeout=500;default command timeout=60";
        Response.Clear();
        foreach (SpocosyXml XmlItem in xmlData.newXml())
        {
            XmlItem.parseData();
            Response.Write(XmlItem.returnMessage + "\n");
        }
        Response.Write("DONE");
    }
</script>