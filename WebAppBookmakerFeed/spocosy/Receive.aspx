<%@ Page Language="C#" ValidateRequest="false" %>
<%@ Import Namespace="Spocosy" %>
<script runat="server">
    protected void Page_Load(object sender, EventArgs e)
    {
        SpocosyXml pushData = new SpocosyXml();
        pushData.connectionString = @"Database=OddsMatching;Data Source=eu-cdbr-azure-north-e.cloudapp.net;User Id=b083cb50265fec;Password=263fb5f7;Connect Timeout=500;default command timeout=60";
        if (!IsPostBack)
        {
            pushData.getPushData();
        }
        Response.Clear();
        Response.Write(pushData.returnMessage);
    }

</script>