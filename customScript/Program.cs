using System;
using System.Data.SqlClient;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;

[Microsoft.SqlServer.Dts.Pipeline.SSISScriptComponentEntryPointAttribute]
public class ScriptMain : UserComponent
{
    private SqlConnection connection;
    private string connectionString;

    public override void PreExecute()
    {
        base.PreExecute();
        connectionString = (string)Variables.ConnectionString;
        connection = new SqlConnection(connectionString);
        connection.Open();
    }

    public override void Input0_ProcessInputRow(Input0Buffer Row)
    {
        // Skip the header row
        if (Row.FirstName == "FirstName")
            return;

        // Generate MemberId
        string memberId = GenerateMemberId();

        // Concatenate the first name, last name, and middle name fields into the FullName field
        string fullName = string.Join(" ", new string[] { Row.FirstName, Row.LastName, Row.MiddleName });

        // Construct a SQL query that inserts the data into the database
        string query = "INSERT INTO tzct_trn_member_import (MemberId, FullName, DOB, Gender, Addr1, Addr2, City, Zip, SSN, CreateID, Createdate, Updateid, Lastupdate, FileFormat) " +
            "VALUES (@MemberId, @FullName, @DOB, @Gender, @Addr1, @Addr2, @City, @Zip, @SSN, @CreateID, @Createdate, @Updateid, @Lastupdate, @FileFormat)";

        // Create a SqlCommand object to execute the query
        using (SqlCommand command = new SqlCommand(query, connection))
        {
            // Set the parameter values for the query
            command.Parameters.AddWithValue("@MemberId", memberId);
            command.Parameters.AddWithValue("@FullName", fullName);
            command.Parameters.AddWithValue("@DOB", Row.DOB);
            command.Parameters.AddWithValue("@Gender", Row.Gender.Substring(0, 1));
            command.Parameters.AddWithValue("@Addr1", Row.Addr1);
            command.Parameters.AddWithValue("@Addr2", Row.Addr2);
            command.Parameters.AddWithValue("@City", Row.City);
            command.Parameters.AddWithValue("@Zip", Row.Zip);
            command.Parameters.AddWithValue("@SSN", Row.SSN);
            command.Parameters.AddWithValue("@CreateID", "DBO");
            command.Parameters.AddWithValue("@Createdate", DateTime.Now);
            command.Parameters.AddWithValue("@Updateid", "DBO");
            command.Parameters.AddWithValue("@Lastupdate", DateTime.Now);
            command.Parameters.AddWithValue("@FileFormat", "FlatFile");

            // Execute the query
            command.ExecuteNonQuery();
        }
    }

    public override void PostExecute()
    {
        base.PostExecute();
        connection.Close();
        connection.Dispose();
    }

    private string GenerateMemberId()
    {
        string memberId = Guid.NewGuid().ToString().Substring(0, 8);
        return memberId;
    }
}