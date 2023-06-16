/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleURLParser;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerURLParser;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Currently SPARK and FLINK do not support cdc")
public class JdbcSqlServerCreateTableIT extends TestSuiteBase implements TestResource {

    private static final String SQLSERVER_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest";
    private static final String SQLSERVER_CONTAINER_HOST = "sqlserver-e2e";
    private static final int SQLSERVER_CONTAINER_PORT = 1433;
    private static final String SQLSERVER_URL =
            "jdbc:sqlserver://" + AbstractJdbcIT.HOST + ":%s;encrypt=false;";
    private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    private static final String CREATE_DATABASE =
            "IF NOT EXISTS (\n"
                    + "   SELECT name \n"
                    + "   FROM sys.databases \n"
                    + "   WHERE name = N'testauto'\n"
                    + ")\n"
                    + "CREATE DATABASE testauto;\n";

    private static final String CREATE_TABLE_SQL =
            "IF NOT EXISTS (SELECT * FROM testauto.sys.tables WHERE name = 'sqlserver_auto_create' AND schema_id = SCHEMA_ID('dbo'))\n"
                    + "BEGIN\n"
                    + "CREATE TABLE testauto.dbo.sqlserver_auto_create (\n"
                    + "  c1 bigint  NOT NULL,\n"
                    + "  c2 bit  NULL,\n"
                    + "  c3 decimal(18)  NULL,\n"
                    + "  c4 decimal(18,2)  NULL,\n"
                    + "  c5 real  NULL,\n"
                    + "  c6 float(53)  NULL,\n"
                    + "  c7 int  NULL,\n"
                    + "  c8 money  NULL,\n"
                    + "  c9 numeric(18)  NULL,\n"
                    + "  c10 numeric(18,2)  NULL,\n"
                    + "  c11 real  NULL,\n"
                    + "  c12 smallint  NULL,\n"
                    + "  c13 smallmoney  NULL,\n"
                    + "  c14 tinyint  NULL,\n"
                    + "  c15 char(10)   NULL,\n"
                    + "  c16 varchar(50)   NULL,\n"
                    + "  c17 varchar(max)   NULL,\n"
                    + "  c18 text   NULL,\n"
                    + "  c19 nchar(10)   NULL,\n"
                    + "  c20 nvarchar(50)   NULL,\n"
                    + "  c21 nvarchar(max)   NULL,\n"
                    + "  c22 ntext   NULL,\n"
                    + "  c25 varbinary(max)  NULL,\n"
                    + "  c26 image  NULL,\n"
                    + "  c27 datetime  NULL,\n"
                    + "  c28 datetime2(7)  NULL,\n"
                    + "  c29 datetimeoffset(7)  NULL,\n"
                    + "  c30 smalldatetime  NULL,\n"
                    + "  c31 date  NULL,\n"
                    + "  PRIMARY KEY CLUSTERED (c1)\n"
                    + ")  \n"
                    + "END";

    private String username;

    private String password;

    private String getInsertSql =
            "INSERT INTO testauto.dbo.sqlserver_auto_create\n"
                    + "(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c25, c26, c27, c28, c29, c30, c31)\n"
                    + "VALUES(8, 1, 714, 876.63, 368.74686, 61.59519333775628, 97, 7.1403, 497, 727.56, 303.78827, 654, 620.8399, 181, N'qEVAoi6KLU', N'1Y7QDYF6me', N'Navicat allows you to transfer data from one database and/or schema to another with detailed analytical process. Instead of wondering when your next vacation is, maybe you should set up a life you don’t need to escape from. I will greet this day with love in my heart. HTTP Tunneling is a method for connecting to a server that uses the same protocol (http://) and the same port (port 80) as a web server does. Export Wizard allows you to export data from tables, collections, views, or query results to any available formats. Always keep your eyes open. Keep watching. Because whatever you see can inspire you. After logged in the Navicat Cloud feature, the Navigation pane will be divided into Navicat Cloud and My Connections sections. Navicat Cloud could not connect and access your databases. By which it means, it could only store your connection settings, queries, model files, and virtual group; your database passwords and data (e.g. tables, views, etc) will not be stored to Navicat Cloud. Always keep your eyes open. Keep watching. Because whatever you see can inspire you. With its well-designed Graphical User Interface(GUI), Navicat lets you quickly and easily create, organize, access and share information in a secure and easy way. Anyone who has ever made anything of importance was disciplined. After logged in the Navicat Cloud feature, the Navigation pane will be divided into Navicat Cloud and My Connections sections. If you wait, all that happens is you get older. Navicat Data Modeler enables you to build high-quality conceptual, logical and physical data models for a wide variety of audiences. Navicat Monitor requires a repository to store alerts and metrics for historical analysis. There is no way to happiness. Happiness is the way. To connect to a database or schema, simply double-click it in the pane. Anyone who has never made a mistake has never tried anything new. If your Internet Service Provider (ISP) does not provide direct access to its server, Secure Tunneling Protocol (SSH) / HTTP is another solution. Navicat 15 has added support for the system-wide dark mode. You will succeed because most people are lazy. Success consists of going from failure to failure without loss of enthusiasm. SSH serves to prevent such vulnerabilities and allows you to access a remote server''s shell without compromising security. Navicat provides a wide range advanced features, such as compelling code editing capabilities, smart code-completion, SQL formatting, and more. Navicat provides powerful tools for working with queries: Query Editor for editing the query text directly, and Query Builder, Find Builder or Aggregate Builder for building queries visually. The Synchronize to Database function will give you a full picture of all database differences. If the plan doesn’t work, change the plan, but never the goal. You can select any connections, objects or projects, and then select the corresponding buttons on the Information Pane. The Main Window consists of several toolbars and panes for you to work on connections, database objects and advanced tools. Actually it is just in an idea when feel oneself can achieve and cannot achieve. The Main Window consists of several toolbars and panes for you to work on connections, database objects and advanced tools. After logged in the Navicat Cloud feature, the Navigation pane will be divided into Navicat Cloud and My Connections sections. Anyone who has never made a mistake has never tried anything new. Navicat Monitor is a safe, simple and agentless remote server monitoring tool that is packed with powerful features to make your monitoring effective as possible. The Main Window consists of several toolbars and panes for you to work on connections, database objects and advanced tools. Navicat provides a wide range advanced features, such as compelling code editing capabilities, smart code-completion, SQL formatting, and more. Champions keep playing until they get it right. If it scares you, it might be a good thing to try. It can also manage cloud databases such as Amazon Redshift, Amazon RDS, Alibaba Cloud. Features in Navicat are sophisticated enough to provide professional developers for all their specific needs, yet easy to learn for users who are new to database server. To connect to a database or schema, simply double-click it in the pane. A query is used to extract data from the database in a readable format according to the user''s request. To successfully establish a new connection to local/remote server - no matter via SSL or SSH, set the database login information in the General tab. SQL Editor allows you to create and edit SQL text, prepare and execute selected queries. Navicat is a multi-connections Database Administration tool allowing you to connect to MySQL, Oracle, PostgreSQL, SQLite, SQL Server, MariaDB and/or MongoDB databases, making database administration to multiple kinds of database so easy. Secure Sockets Layer(SSL) is a protocol for transmitting private documents via the Internet. I may not have gone where I intended to go, but I think I have ended up where I needed to be. Navicat Cloud provides a cloud service for synchronizing connections, queries, model files and virtual group information from Navicat, other Navicat family members, different machines and different platforms. To connect to a database or schema, simply double-click it in the pane. With its well-designed Graphical User Interface(GUI), Navicat lets you quickly and easily create, organize, access and share information in a secure and easy way. I may not have gone where I intended to go, but I think I have ended up where I needed to be. Anyone who has ever made anything of importance was disciplined. Actually it is just in an idea when feel oneself can achieve and cannot achieve. Instead of wondering when your next vacation is, maybe you should set up a life you don’t need to escape from. It wasn’t raining when Noah built the ark. You must be the change you wish to see in the world. SQL Editor allows you to create and edit SQL text, prepare and execute selected queries. Navicat provides a wide range advanced features, such as compelling code editing capabilities, smart code-completion, SQL formatting, and more. To start working with your server in Navicat, you should first establish a connection or several connections using the Connection window. SSH serves to prevent such vulnerabilities and allows you to access a remote server''s shell without compromising security. In the Objects tab, you can use the List List, Detail Detail and ER Diagram ER Diagram buttons to change the object view. Genius is an infinite capacity for taking pains. Typically, it is employed as an encrypted version of Telnet. Secure Sockets Layer(SSL) is a protocol for transmitting private documents via the Internet. You cannot save people, you can just love them. You cannot save people, you can just love them. Navicat provides a wide range advanced features, such as compelling code editing capabilities, smart code-completion, SQL formatting, and more. To connect to a database or schema, simply double-click it in the pane. Navicat provides a wide range advanced features, such as compelling code editing capabilities, smart code-completion, SQL formatting, and more. Navicat Monitor requires a repository to store alerts and metrics for historical analysis. How we spend our days is, of course, how we spend our lives. Instead of wondering when your next vacation is, maybe you should set up a life you don’t need to escape from. To start working with your server in Navicat, you should first establish a connection or several connections using the Connection window. Always keep your eyes open. Keep watching. Because whatever you see can inspire you. Navicat Data Modeler enables you to build high-quality conceptual, logical and physical data models for a wide variety of audiences. Navicat Cloud could not connect and access your databases. By which it means, it could only store your connection settings, queries, model files, and virtual group; your database passwords and data (e.g. tables, views, etc) will not be stored to Navicat Cloud. I may not have gone where I intended to go, but I think I have ended up where I needed to be. The reason why a great man is great is that he resolves to be a great man. Export Wizard allows you to export data from tables, collections, views, or query results to any available formats. Navicat 15 has added support for the system-wide dark mode. Actually it is just in an idea when feel oneself can achieve and cannot achieve. SSH serves to prevent such vulnerabilities and allows you to access a remote server''s shell without compromising security. Difficult circumstances serve as a textbook of life for people. Flexible settings enable you to set up a custom key for comparison and synchronization. It collects process metrics such as CPU load, RAM usage, and a variety of other resources over SSH/SNMP. It wasn’t raining when Noah built the ark. SQL Editor allows you to create and edit SQL text, prepare and execute selected queries. You can select any connections, objects or projects, and then select the corresponding buttons on the Information Pane.', N'Actually it is just in an idea when feel oneself can achieve and cannot achieve. A man is not old until regrets take the place of dreams. With its well-designed Graphical User Interface(GUI), Navicat lets you quickly and easily create, organize, access and share information in a secure and easy way.', N'j8OKNCrsFb', N'KTLmoNjIiI', N'All the Navicat Cloud objects are located under different projects. You can share the project to other Navicat Cloud accounts for collaboration. Navicat Data Modeler is a powerful and cost-effective database design tool which helps you build high-quality conceptual, logical and physical data models. After logged in the Navicat Cloud feature, the Navigation pane will be divided into Navicat Cloud and My Connections sections. Navicat Cloud provides a cloud service for synchronizing connections, queries, model files and virtual group information from Navicat, other Navicat family members, different machines and different platforms. Secure Sockets Layer(SSL) is a protocol for transmitting private documents via the Internet. To successfully establish a new connection to local/remote server - no matter via SSL, SSH or HTTP, set the database login information in the General tab. Champions keep playing until they get it right. It is used while your ISPs do not allow direct connections, but allows establishing HTTP connections. With its well-designed Graphical User Interface(GUI), Navicat lets you quickly and easily create, organize, access and share information in a secure and easy way. Navicat allows you to transfer data from one database and/or schema to another with detailed analytical process. You must be the change you wish to see in the world. Navicat provides a wide range advanced features, such as compelling code editing capabilities, smart code-completion, SQL formatting, and more. Anyone who has never made a mistake has never tried anything new. Navicat allows you to transfer data from one database and/or schema to another with detailed analytical process. I may not have gone where I intended to go, but I think I have ended up where I needed to be. Typically, it is employed as an encrypted version of Telnet. Secure SHell (SSH) is a program to log in into another computer over a network, execute commands on a remote server, and move files from one machine to another. Success consists of going from failure to failure without loss of enthusiasm. Sometimes you win, sometimes you learn. Navicat 15 has added support for the system-wide dark mode. It provides strong authentication and secure encrypted communications between two hosts, known as SSH Port Forwarding (Tunneling), over an insecure network.', N'To connect to a database or schema, simply double-click it in the pane. If you wait, all that happens is you get older. Always keep your eyes open. Keep watching. Because whatever you see can inspire you. Import Wizard allows you to import data to tables/collections from CSV, TXT, XML, DBF and more. Success consists of going from failure to failure without loss of enthusiasm. A query is used to extract data from the database in a readable format according to the user''s request. Anyone who has never made a mistake has never tried anything new. To successfully establish a new connection to local/remote server - no matter via SSL or SSH, set the database login information in the General tab. SQL Editor allows you to create and edit SQL text, prepare and execute selected queries. Navicat Monitor is a safe, simple and agentless remote server monitoring tool that is packed with powerful features to make your monitoring effective as possible. I will greet this day with love in my heart. How we spend our days is, of course, how we spend our lives. You can select any connections, objects or projects, and then select the corresponding buttons on the Information Pane. Remember that failure is an event, not a person. The Information Pane shows the detailed object information, project activities, the DDL of database objects, object dependencies, membership of users/roles and preview. Navicat authorizes you to make connection to remote servers running on different platforms (i.e. Windows, macOS, Linux and UNIX), and supports PAM and GSSAPI authentication. Secure Sockets Layer(SSL) is a protocol for transmitting private documents via the Internet. The Information Pane shows the detailed object information, project activities, the DDL of database objects, object dependencies, membership of users/roles and preview. You can select any connections, objects or projects, and then select the corresponding buttons on the Information Pane. The On Startup feature allows you to control what tabs appear when you launch Navicat. The first step is as good as half over. Always keep your eyes open. Keep watching. Because whatever you see can inspire you. Champions keep playing until they get it right. If the Show objects under schema in navigation pane option is checked at the Preferences window, all database objects are also displayed in the pane. To successfully establish a new connection to local/remote server - no matter via SSL, SSH or HTTP, set the database login information in the General tab. It provides strong authentication and secure encrypted communications between two hosts, known as SSH Port Forwarding (Tunneling), over an insecure network. Navicat is a multi-connections Database Administration tool allowing you to connect to MySQL, Oracle, PostgreSQL, SQLite, SQL Server, MariaDB and/or MongoDB databases, making database administration to multiple kinds of database so easy. It wasn’t raining when Noah built the ark. A comfort zone is a beautiful place, but nothing ever grows there. Navicat Cloud provides a cloud service for synchronizing connections, queries, model files and virtual group information from Navicat, other Navicat family members, different machines and different platforms. The past has no power over the present moment. Creativity is intelligence having fun. Navicat authorizes you to make connection to remote servers running on different platforms (i.e. Windows, macOS, Linux and UNIX), and supports PAM and GSSAPI authentication. HTTP Tunneling is a method for connecting to a server that uses the same protocol (http://) and the same port (port 80) as a web server does. Difficult circumstances serve as a textbook of life for people. A comfort zone is a beautiful place, but nothing ever grows there. I may not have gone where I intended to go, but I think I have ended up where I needed to be. It wasn’t raining when Noah built the ark. Navicat Cloud could not connect and access your databases. By which it means, it could only store your connection settings, queries, model files, and virtual group; your database passwords and data (e.g. tables, views, etc) will not be stored to Navicat Cloud. What you get by achieving your goals is not as important as what you become by achieving your goals. Difficult circumstances serve as a textbook of life for people. There is no way to happiness. Happiness is the way. Genius is an infinite capacity for taking pains. If the plan doesn’t work, change the plan, but never the goal. Genius is an infinite capacity for taking pains.', 0xFFD8FFE000104A46494600010100000100010000FFDB004300080606070605080707070909080A0C140D0C0B0B0C1912130F141D1A1F1E1D1A1C1C20242E2720222C231C1C2837292C30313434341F27393D38323C2E333432FFDB0043010909090C0B0C180D0D1832211C213232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232FFC00011080140014003012200021101031101FFC4001C0001010003000301000000000000000000000705060801020304FFC400441000010302020605070A0309010000000000010203040506110712213141B23651617172153542748191B113142223326282A1C1D152C2F016172433535493A2D2E1FFC4001A010100030101010000000000000000000000030405020601FFC400311101000201020306040602030000000000000102030411051231213233517181133441D114156191B1C152A12442F0FFDA000C03010002110311003F00A8000CB42000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000031376C4D66B1ECB857C5149FE922EB3FB3E8A66B976EE34FAED2ED0C6E54A1B64F3A6796B4AF48D3BD32D6FD09F1E97365EDA55DD715EDD214604827D2E5D9CEFA8B7D1313AA4D77FC150F9B34B57C45FA7456F54EC63D3F98B1F966A3CBFDA5FC36458C130A4D2FB55CD6D65A1513D27C3367EE6AA7EA6DB67C7161BD3DB14158914EECB286A1351CABD49C157B115487268F3E38DED5476C37AF586C4002B2300000035CC478D2DD862AA1A7AD86AA47CACD76AC2C6AA226796DCDC8754C76C96E5AC6F2FB5ACDA7686C60D0FF00BDAB0FFB4B97FC6CFF00D99FC398B2DB89D93AD124CC7C2A88E8E6444764BB953255D84B7D366A579AD5DA1D5B15EB1BCC33A0020700000000003E35752CA3A39EAA4472B218DD2391BBD511335CBDC693FDED587FDA5CBFE367FEC971E0C997B691BBAAD2D6EEC37C06A763D20DA6FF00758EDD4B4F5AC9A4472A3A56311BB133E0E5EA36C39C98AF8E796F1B496ACD676900070E40000000000000000000000FCD5F5F4D6CA09AB6AE548A085BACF72FF5BD57622088999DA0EAF171B951DAA89F575D3B21819BDCE5DFD889C57B1091626D255C2E6E7D35A95F45499AA7CA22E52BD3B57D1F67BCC1E29C515789AE4E9A473994AC5CA0833D8C4EB5EB72F153027A0D270FAE388B64ED9FE1A1874F15EDB7579739CF72B9CAAAE55CD55576AA9E0D830EE0EBAE247A3E9A248A95172754CBB189DDC5CBDDEDC8A45B345963A4622D73E7AE932DBACE58D9EC46AE7F9A96336B70E19DAD3DBE5092F9E94EC945C1D110612C3D4EC4632CB42A89FEA428F5F7BB353DA4C2D87E46EABACB6F44FBB4ED6FC10A9F9B63FF001945F8BAF939D416DB968C30FD6B55695B350C9B72589EAE6AAF6A3B3F722A133C4983AE786A4D6A8624D48E5C99531A7D155EA5EA5FEB696F06BB0E69DAB3B4FEA9699E97EC86430B6906E1637B29EB1CFACB7A6CD472E6F8D3EEAAFC176756459ADD71A4BAD0C7594533658244CD1C9C3B17A97B0E68366C198AE6C35736A48E73ADF33B29E3DF97DF4ED4FCD3675655F5BA0AE489BE38DADFCA3CD822D1CD5EABD83D63919344C9637A3E37B51CD735734545DCA87B1E7D9E122D2EF9E6DFEAEBCCA574916977CF36FF00575E652F70DF988F74FA6F1213B32787EF75187EF305C29F6EA2E52333D8F62EF6FF005C723180F496AC5A26B3D25A531131B4BA6682BA9EE541056D2BF5E09988F63BB3F73F411FD19E29F27D7791AAE4CA9AA5D9C2E72FD893ABB9DF1CBAD4B01E5755A79C1926B3D3E8CACB8E71DB600057460000C7DF7A3D72F5597914E6E3A46FBD1EB97AACBC8A7371B9C23B965ED27496DBA35E9C51F824E452E842F46BD38A3F049C8A5D0ABC57C78F4FBA2D577FD80019AAC00000000000000000000120D286237565C9B65A77AFCDE9575A6C9763E45E1F853F355EA2A379B8B2D166ACB83F25482257A22F15E09ED5C90E6F9A692A2792695EAF92472BDEE5DEAAAB9AA9ABC2F045AF3927E9FCAD6971EF3CD3F47A1BB603C17E5F9D6BEBDAE6DBA17648DDCB3BBA93B138AFB138E5A9DB6825BA5CE9A861FF00327912345EACD77FB379D1D6FA082D96F828A99BAB0C0C46353F55ED5DE5DE23AA9C34E5A7594FA8CB348DA3ACBED1451C10B2186364713111AD631A888D44DC8889B90F700F3ACE00000F8D552C15B4B25354C4D96191BAAF639334543EC044EDDB039FF18E1A7E19BD3A06E6EA49915F4EF5FE1E2D5ED4FD9789AF17AC7D644BCE16A8D4667514A8B3C59266AB926D4F6A67B3AF2221476CAFB8AAA51515454E4B92FC8C4AECBBF243D368B53F1716F69ED8EAD3C3979E9BCAAFA2DBF3ABAD32DAA77AACB47B62555DAB1AF0F62FE4A886FE4A702E11C4768C41057D452B29E9B55CC952495359CD54E0899EDCF25DB96E2AC626BA2919A671CEF13E4A59E2BCFBD4245A5DF3CDBFD5D7994AE922D2EF9E6DFEAEBCCA77C37E623DDF74DE242767DA6A59E9E385F2C6AD64ECF948D57739B9AA669ED453E25629F0CB71268B6DAD89A9F3EA763DF4EEEB5D7766DEE5F8E46F67CF187966DD26765FC99229B6E9422AA2A2A2AA2A6E542EB80F142621B324750FCEBE95119367BDE9C1FEDE3DBEC214F63A37B98F6AB5CD5C95AA992A2F5192C3F7BA8C3F7982E14FB751729199EC7B177B7FAE3911EB34D19F1ED1D63A39CD8FE257F574703F3D05753DCA820ADA57EBC13311EC7767EE7E83CBCC4C4ED2CBE80000C7DF7A3D72F5597914E6E3A46FBD1EB97AACBC8A7371B9C23B965ED27496DBA35E9C51F824E452E842F46BD38A3F049C8A5D0ABC57C78F4FBA2D577FD80019AAC00000000000000000000D0F4AD5CB4F8661A46B9116A67447275B5A99FC7548D14AD2FCDAD5B6A83F82391FEF544FE526A7A5E1D4E5D3C7EAD3D346D8E1BE68AADA95588E6AD7A66DA48736F63DDB13F2D62CA4EB4454C8CB35C2AB2DB254246BF85A8BFCE514C7E237E6D44FE9D8A7A8B6F924001490000000000A88A992ED43C22235A88888889B111381E4000000245A5DF3CDBFD5D7994AE922D2EF9E6DFEAEBCCA5EE1BF311EE9F4DE242765F700F41ED9E0773B88117DC03D07B6781DCEE3478B7831EBFD4AC6AFB91EAD2749D85BE6D51E5DA48FEA6672254B53D17F07772F1EDEF27074DD5D2C35D492D2D4C692432B558F6AF14539F3135826C397A96865CDD1FDA8645F4D8BB97BF82F6A0E1BAAE7AFC2B758FE1F74D979A3967AC365D1AE29F265C7C91572654954EFAA739764727ECED89DF9769643978B9E00C53FDA0B47C854C99DC29511B2AAEF91BC1FFA2F6F7A10713D2ED3F1ABEFF747AAC5FF00786DC0031D4D8FBEF47AE5EAB2F229CDC748DF7A3D72F5597914E6E37384772CBDA4E92DB746BD38A3F049C8A5D085E8D7A7147E093914BA1578AF8F1E9F745AAEFF00B00033558000000000000000000011DD2DBF3C4B46CCB751B573EF7BFF00634028BA5D8952F36F9783A9D5BEE77FF49D1EA7433FF1EAD4C1E1C2D7A2C6A3707AAA7A552F55F7221BB1A2E8A25D7C293B38C756F4F7B5ABFA9BD1E7F59E3DFD59F9BC4900056460000000000000000122D2EF9E6DFEAEBCCA574916977CF36FF575E652F70DF988F74FA6F1213B2FB807A0F6CF03B9DC408BEE01E83DB3C0EE771A3C5BC18F5FEA56357DC8F56C86B58DB0CB712595CD89A9F3EA7CDF4EEEB5E2DEE5F8E46CA0C3C792D8ED17AF5851ADA6B3BC397DEC746F731ED56B9AB92B5532545EA32162BCD4586EF05C29B6BA35C9CC55C91ED5DED5FEB7E4BC0DDB49D85BE6D51E5DA48FEA6672254B53D17F07772F1EDEF2707A9C5929A8C5CDF49EAD5A5A3257774BDBAE14D75B7C15D48FD78266EB357E28BDA8B9A2F71FA88DE8D714F932E3E48AB932A4AA77D539CBB2393F676C4EFCBB4B21E6F55A79C1926BF4FA3372E39C76D98FBEF47AE5EAB2F229CDC748DF7A3D72F5597914E6E35384772CB5A4E92DB746BD38A3F049C8A5D085E8D7A7147E093914BA1578AF8F1E9F745AAEFF00B000335580000000000000000000135D2F522BA82D9589BA395F12FE24454E452505F71EDBBCA583AB98D4CE485A93B3667F676AFE599023D170CBF360E5F2968E96DBD36F254B443588B1DD289576A2B256A7BD17E0D29E41B47D744B5E2FA557B91B154A2D3BD57EF6597FD91A5E4CCE278F973CCF9AB6A6BB64DFCC001415C000000003D5AF63F3D4735D92AB5725CF254E07A54D445474B2D4CEE46C51315EF72F0444CD4E709EE9572DD6A2E31CD2C13CD23A45746F5454D65CF2CD0B9A4D1CEA37EDDB64D8B0CE4DDD280845AF1E628A79A1822AE75566E463639D88FD655D889ADF6BF32EACD7F936FCA6AEBE49ADABBB3E391C6A74B7D3CC734C76BE65C538FABD8916977CF36FF575E652BA48B4BBE79B7FABAF3292F0DF988F775A6F1213B2FB807A0F6CF03B9DC408BEE01E83DB3C0EE771A3C5BC18F5FEA56357DC8F56C80030141F1ABA586BA925A5A98D248656AB1ED5E28A73E626B04D872F52D0CB9BA3FB50C8BE9B1772F7F05ED43A24D6B1B6196E24B2B9B1353E7D4F9BE9DDD6BC5BDCBF1C8BDA0D57C1C9B5BBB3FF00B74F832F25B69E9281973C018A7FB4168F90A9933B852A236555DF23783FF45EDEF421AF63A37B98F6AB5CD5C95AA992A2F5190B15E6A2C37782E14DB5D1AE4E62AE48F6AEF6AFF5BF25E06D6B34D19F1ED1D63A2EE6C7F12BB7D5D017DE8F5CBD565E4539B8E88ACB8535D7075657523F5E09A8A57357F0AE68BDA8B9A2F71CEE54E131315BC4F9A1D246D12DB746BD38A3F049C8A5D085E8D7A7147E093914BA1538AF8F1E9F745AAEFF00B0003355800000000000000000001E1CD6BDAAD72239AA992A2A668A873A624B43AC5882AE8151518C7E712AF162ED6AFBBF3453A30D174958656ED6B4B9D2C7AD5746DFA4889B5F16F54F66D5F797F876A23165E5B74958D364E5B6D3F5465AE731C8E6AAA391734545DA8A5FB06E248F11D8D92B9EDF9E4288CA9626F4770765D4B967EF4E0400C958EF95B87EE4CADA27A23D1355EC77D97B7A950D8D6E97F114DA3AC745CCD8BE257F5747835FC398C2D789226A412A4557966FA6917E92777F1276A7B723603CD5E96A5B96D1B4B32D59ACED2000E5F000D4716E3BA2C3F1494D4CE654DCB2C92245CDB1AF5BD7F4DFDDBCEF1E2BE5B72D2379755ACDA76862B4A3889B4B6E6D969E4FF115393A6CBD18D3877AAA7B917AC909F6ABABA8AFAB96AEAA574B3CAED67BDDBD54F9318E91ED631AAE7397246A266AABD47A8D3608C18E29FBB4F1638C75D9B6E8E2CEEB9E2A867735160A2FAF7AAFF17A09DF9EDF617335AC1187530ED8238E56E5593E52D42F145E0DF626CEFCCD94C0D767F8D9A663A47642867C9CF7ECE8122D2EF9E6DFEAEBCCA574916977CF36FF00575E653AE1BF311EEFBA6F1213B2FB807A0F6CF03B9DC408BEE01E83DB3C0EE771A3C5BC18F5FEA56357DC8F56C80030140000125D27616F9B547976923FA999C8952D4F45FC1DDCBC7B7BC9C1D37574B0D7524B4B531A490CAD563DABC514E7CC4D609B0E5EA5A1973747F6A1917D362EE5EFE0BDA86FF0DD573D7E15BAC7F0D0D365E68E59EB0CB612C51E4DB7DCAD156FFF0009554F2FC92AAEC8E4D45FC9DB13BF2ED35100D0AE3AD6D368FAAC456226663EADB746BD38A3F049C8A5D085E8D7A7147E093914BA185C57C78F4FBA86ABBFEC000CD560000000000000000000000004871EE047DBE496EF6A8D5D48E557CF0B536C2BC5C9F77E1DDBA787509A1E25D19D0DD1CFAAB53D94554BB56354FAA7AF727D9F66CEC36747C4A2239337EFF75CC3A9DA396E8DB5CE6391CD554722E68A8BB514DAAD7A45C456C8D235A9655C6D4C91B54DD754FC48A8E5F6A98DBAE15BDD955CB5B6F99B1B76FCB3135E3CBAF593627B72530C6ACD7167AF6ED685B98ADE3CD4C8B4C13237EBACB1BDDD6CA856A7E6D513E97E7731529ECD1C6FE0B2542BD3DC8D4266083F2FD36FBF2FFB947F87C7E4DA2EBA41C457563A3755A52C4EDECA56EA7FDB6BBF33570676CF83AF97BD57D2D0BDB03B2FAF9BE8332EB455DFECCC9E23160AFD2B0936AD23C9822B1A3FC0AFA47C77ABB44AD9D36D3D3BD36B3EFB93AFA9386FDFBB3385F47D6FB03995552A9595EDDA9239B93235FBA9D7DABB7B8DC0C7D6711E789C78BA79A9E6D473472D000192A8122D2EF9E6DFEAEBCCA574916977CF36FF00575E652F70DF988F74FA6F1213B2FB807A0F6CF03B9DC408BEE01E83DB3C0EE771A3C5BC18F5FEA56357DC8F56C80030140000035AC6D865B892CAE6C4D4F9F53E6FA7775AF16F72FC723650778F25B1DA2F5EB0FB5B4D677872FBD8E8DEE63DAAD7357256AA64A8BD47828FA4EC2DF36A8F2ED247F5333912A5A9E8BF83BB978F6F79383D560CD5CD8E2F56B63BC5EBCD0DB746BD38A3F049C8A5D085E8D7A7147E093914BA189C57C78F4FBA8EABBFEC000CD5600000000000000000000000000000C7D5D86D15EED6AAB6524CFF00E27C2D577BF2CCC803EC5A6BDB1244CC746B52600C2F2FDAB4B13C32BDBF071E19A3EC2D1BB59B6A6AAFDE9A4727B95C6CC097F119BFCE7F7977F12FE72C751582CF6E735F476CA48646EC491B126B27E2DE644022B5A6D3BCCEEE26667A8003E00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000FFD9, 0xFFD8FFE000104A46494600010100000100010000FFDB004300080606070605080707070909080A0C140D0C0B0B0C1912130F141D1A1F1E1D1A1C1C20242E2720222C231C1C2837292C30313434341F27393D38323C2E333432FFDB0043010909090C0B0C180D0D1832211C213232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232FFC00011080140014003012200021101031101FFC4001C0001000301010101010000000000000000000506070804030201FFC40049100100010302020507070906040700000000010203040511063107213641615171727481B1B2121314152223911735547393A1B3C1D224528292D1F03242626325335355A2A4E2FFC400190101000301010000000000000000000000000304050201FFC400251101000201030304030100000000000000000102030411321231332141718113226151FFDA000C03010002110311003F00D24064AD000000000000000000000000000000000000000000000000000000000000000004F5472DC0148C8E94347C4C9B98F7F0353B77AD5534574556A8DE2639C7FC6F9FE56342FD1352FD9D1FD697F064FF00117E6C7FEAF622B40E20C1E23D3E733066B8A69AA68AEDDC888AE89F18899F3A551CC4C4ED29226263780078F400000011FAD6B389A0E99733F36AAA2D5131114D11BD554CF2888EF9553F2B1A17E89A97ECE8FEB775C57B46F58716C95ACED32BD8A4E374A1A46664DBC7C7C0D4EE5EB954534514DAA37999FF001AED1D71CB6797A5A9CA1ED6F5B719079751CEB5A669D919D7A9AEAB562DCDCAA28889AA623C9BA9DF958D0BF44D4BF6747F5BDAE3BDFD6B0F2D92B5E52BD8AA689D20695AF6A96F4FC5C7CDA2F5C8AA62ABB45314F546FDD54F916B796A5AB3B5A1ED6D168DE001CBA000000054758E91348D1755BFA764E3E6D77ACCC45555BA289A677889EADEA8F2BC5F958D0BF44D4BF6747F5A58C192637884739A913B4CAF63E183976F3F031B32D45516F22D537688AA3AE22A8898DFC7ADF745D9D800F40000000000000019A749DC2FF396FEBEC3B7F6E888A72A9A639C728AFD9CA7C36F232C74E5CB745EB55DAB94C576EBA669AA9AA378989E712C138C786EBE1BD6ABB34C4CE25EDEBC7AE7FBBDF4CF8C72FC27BDA3A4CDBC744A86A716D3D70FEF06F11D5C39AE517ABAA7E877B6A322988DFECF755B79639FE3E56F74574DCA29AE8AA2AA2A8DE9AA99DE263CB0E626B1D187134E4E3CE859773EF6CD3F2B1A667AEAA3BE9F67778798D5E1DE3AE0D2E5DA7A25A300CE5F0000145E9238A3EABD3BEAAC4B9B666553F6EAA67AEDDBEFF6CF5C79B7F0778E937B4561C5EF14AF54A91C7DC4DF5F6B33631AE7CAC0C599A6D4C4F557577D5FCA3C23C552167E08E18AF88B5889BD44FD031E62ABF57F7BC94479FDDBF835FF005C54FE432FF6C97FECAE1D19F0ACE359FAF732DFDEDDA76C5A663AE9A279D7EDE51E1BF95A3BF94D34D14C534C4534C46D1111B4443FAC8C992725BAA5A98E914AF4C21B8B7B23AB7AB57EE73D3A178B7B23AB7AB57EE73D2F68B8CA9EAF942D9D1BF6DF0FD0B9F04B7261BD1BF6DF0FD0B9F04B7243ACF27D25D270FB00545A00000060DC7FDB8D4FD2A3F874AB4B2F1FF6E353F4A8FE1D2AD36B170AFC43232739F974570D765B48F52B3F0425117C35D96D23D4ACFC109463DF94B56BC6001CBA00000000000000010BC53C3F6B88F45BB8756D4DFA7EDD8B93FF002D71CBD93CA5343DADA6B3BC3C988B46D2E65C8C7BB8993731EFDB9B77AD553457455CE2639C3F787977F0332CE5E35C9B77ACD715D154774C34CE93B85FE72DFD7D876FEDD1114E5534C738E515FB394F86DE4658D9C592325376564A4E3B6CE89E1CD72CF10E8B633ED6D4D557D9BB6E277F915C738FE71E13095615C0DC4F3C3DACC517EB9FA064CC517A3BA89EEAFD9DFE133E0DD62626378EB865E7C5F8EDFC6861C9F92BFD004299E1D6355C7D134ABFA8654FDDDAA778A639D73DD4C78CCB9EB54D4B2357D4F233F2AADEF5FAFE54EDCA23944478446D1EC5A3A43E279D6B56FA0E357BE0E255311B72B97394D5E68E51EDF2A98D4D2E1E8AF54F7966EA32F5DB68ED0F4E9F8191A9EA1630B168F977EF5514D31FCE7C239BA0787B44B1C3FA359C0B1B4CD31F2AED7B6DF395CF3ABFDF7442AFD1BF0ACE9983F5B6651B65E4D3F754CC75DBB7FEB3CFCDB78AF8ADAACDD73D31DA1634D8BA63AA7BC802A2D21B8B7B23AB7AB57EE73D3A178B7B23AB7AB57EE73D34745C654357CA16CE8DFB6F87E85CF825B930DE8DFB6F87E85CF825B921D6793E92E9387D802A2D000000306E3FEDC6A7E951FC3A55A5978FFB71A9FA547F0E9569B58B857E2191939CFCBA2B86BB2DA47A959F821288BE1AECB691EA567E084A31EFCA5AB5E3000E5D0000000000000000003F372DD17AD576AE5315DBAE99A6AA6A8DE26279C4B01E2EE1DB9C37ADDCC6DA6716E6F731EB9EFA37E533E58E53EC9EF74020B8B7876DF126897317AA326DFDE63D73DD5EDCA7C2794FE3DC9F4F97F1DBD7B4A0CF8BAEBE9DDCFCD8BA35E269D4B4E9D272AE6F958B4C7CD4CCF5D76FFD69EA8F34C78B20BD66E63DFB962F51345DB754D15D1573A6A89DA625E9D2B52BFA46A98F9F8D3B5DB15FCA8F18E531E698DE3DAD2CD8E3253651C59271DB7749299D21F134E8BA47D0B1AE6D9B99134C4C4F5DBB7CA6AF3F747B67B960B3AEE0DEE1E8D6FE76230FE666ECCF7C6DCE3CFBC4C6DE560BAEEB17F5DD632350BFD5372AFB34F7514C754447B1434D87AAFBDBB42E6A32F4D768F7472E1C01C2BF5F6A7F4BCAA2274FC5AA26B8AA3AAE55CE29F37299F0EAEF57748D2B235AD52C6062D3BDCBB56DBF7531DF54F84475BA0B47D2B1F44D2AC69F8B1F776A9DA6A9E75CF7D53E332B5A9CDD15E98EF2ADA7C5D73BCF687B8065B480010DC5BD91D5BD5ABF739E9D0BC5BD91D5BD5ABF739E9A3A2E32A1ABE50B6746FDB7C3F42E7C12DC986F46FDB7C3F42E7C12DC90EB3C9F49749C3EC0151680000018371FF6E353F4A8FE1D2AD2CBC7FDB8D4FD2A3F874AB4DAC5C2BF10C8C9CE7E5D15C35D96D23D4ACFC109445F0D765B48F52B3F042518F7E52D5AF180072E800000000000000000000198F49DC2FBC7D7F876FAE36A72A9A63F0AFDD13ECF165EE9BBD66DE458B966F514D76AE5334574551BC55131B4C4B02E2EE1DAF86F5CB98D1BD58D73EF31EB9EFA26794F8C729FC7BDA3A4CDD51D12A1A9C5B4F5C3C14EB39B4E87568F177FB1D57A2F4D3DFF002B6E5E6EFDBCAF00BDF471C2D1AAE7CEAB9746F898B5C7CDD33CAE5CE7F84754FB63C566F6AE3ACDA55EB59BDA2AB8F47DC2BF5169BF4DCAA36CFCAA6266263AED51CE29F3F7CFB23B972063DEF37B754B56958AC6D000E5D00021B8B7B23AB7AB57EE73D3A178B7B23AB7AB57EE73D34745C654357CA16CE8DFB6F87E85CF825B930DE8DFB6F87E85CF825B921D6793E92E9387D802A2D000000306E3FEDC6A7E951FC3A55A5978FF00B71A9FA547F0E9569B58B857E2191939CFCBA2B86BB2DA47A959F821288BE1AECB691EA567E084A31EFCA5AB5E3000E5D0000000000000000000000C9FA5CFCE5A6FEA6BF7C35864FD2E7E72D37F535FBE1634BE5841A9F1CB396CFD16764ABF5AAFDD4B186CFD16764ABF5AAFDD4AE6B3C6A9A5F22EE032DA400000087E2CA66AE12D5A23F45B93FB9CF2E91D62CFD2744CFB1FFAB8D728FC6998737347453FACC286AE3F685A3A3BAE69E39D3E37DA2A8B913FB3A9BB39F783B263178C34BB957544DF8A3FCDF67F9BA0916B63F789FE25D24FE9200A6B4000000C0F8EEBF97C6DA9CFFDCA63F0A2985752BC4D91F4BE28D52F44EF1564DC889F08AA623F74229B78E36A4431EF3BDA65D17C3B4CD1C31A4D3546D31876627FC909379F06CFD1B4FC6B1B6DF376A9A36F34443D0C5B4EF332D7AC6D1000F1E8000000000000000000000C9FA5CFCE5A6FEA6BF7C35864FD2E7E72D37F535FBE1634BE5841A9F1CB396CFD16764ABF5AAFDD4B186CFD16764ABF5AAFDD4AE6B3C6A9A5F22EE032DA40000004C44C6D3D70E6DD5B0A74DD5F330A63FF0022F556E3CD13D5FB9D24C77A52D1E70F5EB7A9514CFCD6653B55311D515D31113F8C6DFBD6F477DAF35FF557555DEB13FE28F62F578F916EFDB9DAE5BAA2BA67C9313BC3A474ECEB5A9E9D8F9D6277B57EDC574F86F1CBCF1C9CD4BD700F1AD1A255F566A354C605CAB7A2E6DBFCD553CF7FFA67F77E2B3AAC537AEF1DE1069B2452DB4F696C83F16AEDBBD6A9BB6AE5372DD51BD35513BC4C784BF6CB6880008FD7353B7A3E89979F72A88F99B7334EFDF572A63DB3B43DB76EDBB36AABB76E536EDD31BD55573B44478CB1CE3FE32A35DBB4E9DA7D754E0DAABE5575F2F9EABBBFC31FEFB92E1C5392DB7B22CB92295DFDD489999999999999E73291E1FC29D438874FC588DFE72FD1157A3BEF3FBB746B43E8AB45AAFEA77F58B94FDD635336AD4CC73B95475EDE6A67FF00943572DFA2932CDC75EABC435B018AD7000000000000000000000000193F4B9F9CB4DFD4D7EF86B0C9FA5CFCE5A6FEA6BF7C2C697CB08353E39672D9FA2CEC957EB55FBA9630D9FA2CEC957EB55FBA95CD678D534BE45DC065B4800000046EBDA2E3F106917B4FC999A62BEBA2B88DE68AA39551FEFCA921EC4CC4EF0F262263697376ADA4E668BA8DCC1CEB5345DA394F7571DD5533DF12F13A275DE1DD3B88B0FE8F9D6779A77F9BBB4F5576E7C27F972651AE746FACE99555730E9FAC31E3AE26D47DB8F3D1CE7D9BB4F16A6B78DADE92CECBA7B56778F5841E91C4BABE873FF87E6DCB76FBED4ED5513FE19EAF6C75ADF8BD2DE7D14ED97A663DE9F2DAAEAB7EFF0094CF6ED9BB8F76AB57ADD76EE533B554574CC4C4F8C4BF09AD8B1DFD6611D72DEBE912D3E7A5F9DBAB43EBF5BFFF000F0E5F4B5A9DC8DB134FC5B3E3726AB93FC99F0E234D8A3D9D4EA324FBA5756E24D635B9DB50CEBB768EEB71B5347F96368F6A29F4B18F7F2AF53671ECDCBD76AFF868B74CD554FB2175D07A32D5350AE9BBA9FF0061C6E7F26769BB579A3BBDBD7E0EED6A638F5F47115BE49F4F556B40D033788B52A70F0E9EAE772ECC7D9B74F967FD3BDBEE93A663E8DA5D8C0C5A76B5669DB79E754F7D53E333D6FE693A3E0E89854E26058A6D5B8EB99FF9AB9F2D53DF2F73373E79CB3B4766861C318E379EE00AE9C000000000000000000000000000000000000000000001F0C9C2C5CDA3E4656359BF47F76EDB8AA3F7A22F70570DDF9DEBD231E27FE889A3DD309E1D45AD1DA5CCD6B3DE158FC9DF0AFF00ED7FFD8BBFD4F558E0BE1BC7AFE551A4634CFF00DC89AE3F0AB74E8F672DE7DE5E7E3A47B43E38D898D876A2D62E3DAB16E39516A88A63F087D81C3B0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000007FFD9, '2006-02-27 05:15:03.000', '2019-08-14 17:36:43.000', N'2003-05-14 08:07:42 +00:00', '1900-06-19 00:00:00.000', '2005-05-29');\n";

    private static final String PG_IMAGE = "postgis/postgis";
    private static final String PG_DRIVER_JAR =
            "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar";
    private static final String PG_JDBC_JAR =
            "https://repo1.maven.org/maven2/net/postgis/postgis-jdbc/2.5.1/postgis-jdbc-2.5.1.jar";
    private static final String PG_GEOMETRY_JAR =
            "https://repo1.maven.org/maven2/net/postgis/postgis-geometry/2.5.1/postgis-geometry-2.5.1.jar";

    private static final String MYSQL_IMAGE = "mysql:latest";
    private static final String MYSQL_CONTAINER_HOST = "mysql-e2e";
    private static final String MYSQL_DATABASE = "auto";

    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";
    private static final int MYSQL_PORT = 3306;
    //    private static final String MYSQL_URL = "jdbc:mysql://" + HOST + ":%s/%s?useSSL=false";

    private static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";

    private static final String ORACLE_IMAGE = "gvenzl/oracle-xe:21-slim-faststart";
    private static final String ORACLE_NETWORK_ALIASES = "e2e_oracleDb";
    private static final String ORACLE_DRIVER_CLASS = "oracle.jdbc.OracleDriver";
    private static final int ORACLE_PORT = 1521;
    //    private static final String ORACLE_URL = "jdbc:oracle:thin:@" + HOST + ":%s/%s";
    private static final String USERNAME = "testUser";
    private static final String PASSWORD = "Abc!@#135_seatunnel";
    private static final String DATABASE = "TESTUSER";
    private static final String SOURCE_TABLE = "E2E_TABLE_SOURCE";
    private static final String SINK_TABLE = "E2E_TABLE_SINK";

    private PostgreSQLContainer<?> POSTGRESQL_CONTAINER;

    private MSSQLServerContainer<?> sqlserver_container;
    private MySQLContainer<?> mysql_container;
    private OracleContainer oracle_container;

    private static final String mysqlCheck =
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'auto' AND table_name = 'sqlserver_auto_create_mysql') AS table_exists";
    private static final String sqlserverCheck =
            "IF EXISTS (\n"
                    + "    SELECT 1\n"
                    + "    FROM testauto.sys.tables t\n"
                    + "    JOIN testauto.sys.schemas s ON t.schema_id = s.schema_id\n"
                    + "    WHERE t.name = 'sqlserver_auto_create_sql' AND s.name = 'dbo'\n"
                    + ")\n"
                    + "    SELECT 1 AS table_exists;\n"
                    + "ELSE\n"
                    + "    SELECT 0 AS table_exists;";
    private static final String pgCheck =
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sqlserver_auto_create_pg') AS table_exists;\n";
    private static final String oracleCheck =
            "SELECT CASE WHEN EXISTS(SELECT 1 FROM user_tables WHERE table_name = 'sqlserver_auto_create_oracle') THEN 1 ELSE 0 END AS table_exists FROM DUAL;\n";

    String driverMySqlUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    String driverOracleUrl() {
        return "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/12.2.0.1/ojdbc8-12.2.0.1.jar";
    }

    String driverSqlserverUrl() {
        return "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/9.4.1.jre8/mssql-jdbc-9.4.1.jre8.jar";
    }

    static JdbcUrlUtil.UrlInfo sqlParse =
            SqlServerURLParser.parse("jdbc:sqlserver://localhost:1433;database=testauto");
    static JdbcUrlUtil.UrlInfo MysqlUrlInfo =
            JdbcUrlUtil.getUrlInfo("jdbc:mysql://localhost:3306/auto?useSSL=false");
    static JdbcUrlUtil.UrlInfo pg = JdbcUrlUtil.getUrlInfo("jdbc:postgresql://localhost:5432/pg");
    static JdbcUrlUtil.UrlInfo oracle =
            OracleURLParser.parse("jdbc:oracle:thin:@localhost:1521/TESTUSER");

    @TestContainerExtension
    private final ContainerExtendedFactory extendedSqlServerFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                                        + PG_DRIVER_JAR
                                        + " && curl -O "
                                        + PG_JDBC_JAR
                                        + " && curl -O "
                                        + PG_GEOMETRY_JAR
                                        + " && curl -O "
                                        + MYSQL_DRIVER_CLASS
                                        + " && curl -O "
                                        + ORACLE_DRIVER_CLASS
                                        + " && curl -O "
                                        + driverSqlserverUrl()
                                        + " && curl -O "
                                        + driverMySqlUrl()
                                        + " && curl -O "
                                        + driverOracleUrl());
                //                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    void initContainer() throws ClassNotFoundException {
        DockerImageName imageName = DockerImageName.parse(SQLSERVER_IMAGE);
        sqlserver_container =
                new MSSQLServerContainer<>(imageName)
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases(SQLSERVER_CONTAINER_HOST)
                        .withPassword(PASSWORD)
                        .acceptLicense()
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(SQLSERVER_IMAGE)));

        sqlserver_container.setPortBindings(
                Lists.newArrayList(
                        String.format(
                                "%s:%s", SQLSERVER_CONTAINER_PORT, SQLSERVER_CONTAINER_PORT)));

        try {
            Class.forName(sqlserver_container.getDriverClassName());
        } catch (ClassNotFoundException e) {
            throw new SeaTunnelRuntimeException(
                    JdbcITErrorCode.DRIVER_NOT_FOUND, "Not found suitable driver for mssql", e);
        }

        username = sqlserver_container.getUsername();
        password = sqlserver_container.getPassword();
        // ============= PG
        POSTGRESQL_CONTAINER =
                new PostgreSQLContainer<>(
                                DockerImageName.parse(PG_IMAGE)
                                        .asCompatibleSubstituteFor("postgres"))
                        .withNetwork(TestSuiteBase.NETWORK)
                        .withNetworkAliases("postgre-e2e")
                        .withDatabaseName("pg")
                        .withUsername(USERNAME)
                        .withPassword(PASSWORD)
                        .withCommand("postgres -c max_prepared_transactions=100")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PG_IMAGE)));
        POSTGRESQL_CONTAINER.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", 5432, 5432)));

        log.info("PostgreSQL container started");
        Class.forName(POSTGRESQL_CONTAINER.getDriverClassName());

        log.info("pg data initialization succeeded. Procedure");
        DockerImageName mysqlImageName = DockerImageName.parse(MYSQL_IMAGE);
        mysql_container =
                new MySQLContainer<>(mysqlImageName)
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(MYSQL_PASSWORD)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_CONTAINER_HOST)
                        .withExposedPorts(MYSQL_PORT)
                        .waitingFor(Wait.forHealthcheck())
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MYSQL_IMAGE)));

        mysql_container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", MYSQL_PORT, MYSQL_PORT)));

        DockerImageName oracleImageName = DockerImageName.parse(ORACLE_IMAGE);
        oracle_container =
                new OracleContainer(oracleImageName)
                        .withDatabaseName(DATABASE)
                        .withUsername(USERNAME)
                        .withPassword(PASSWORD)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(ORACLE_NETWORK_ALIASES)
                        .withExposedPorts(ORACLE_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(ORACLE_IMAGE)));
        oracle_container.withCommand(
                "bash",
                "-c",
                "echo \"CREATE USER admin IDENTIFIED BY admin; GRANT DBA TO admin;\" | sqlplus / as sysdba");
        oracle_container.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", ORACLE_PORT, ORACLE_PORT)));
        Startables.deepStart(
                        Stream.of(
                                POSTGRESQL_CONTAINER,
                                sqlserver_container,
                                mysql_container,
                                oracle_container))
                .join();

        log.info(" container is up ");
    }

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        initContainer();

        initializeJdbcTable();
    }

    @TestTemplate
    public void testAutoCreateTable(TestContainer container)
            throws IOException, InterruptedException {

        TablePath tablePathSQL = TablePath.of("testauto", "dbo", "sqlserver_auto_create");
        TablePath tablePathSQL_Sql = TablePath.of("testauto", "dbo", "sqlserver_auto_create_sql");
        TablePath tablePathMySql = TablePath.of("auto", "sqlserver_auto_create_mysql");
        TablePath tablePathPG = TablePath.of("pg", "public", "sqlserver_auto_create_pg");
        TablePath tablePathOracle = TablePath.of("TESTUSER", "sqlserver_auto_create_oracle");

        SqlServerCatalog sqlServerCatalog =
                new SqlServerCatalog("sqlserver", "sa", password, sqlParse, "dbo");
        MySqlCatalog mySqlCatalog = new MySqlCatalog("mysql", "root", PASSWORD, MysqlUrlInfo);
        PostgresCatalog postgresCatalog =
                new PostgresCatalog("postgres", "testUser", PASSWORD, pg, "public");
        OracleCatalog oracleCatalog =
                new OracleCatalog("oracle", "admin", "admin", oracle, "TESTUSER");
        mySqlCatalog.open();
        sqlServerCatalog.open();
        postgresCatalog.open();
        //        oracleCatalog.open();

        CatalogTable sqlServerCatalogTable = sqlServerCatalog.getTable(tablePathSQL);

        sqlServerCatalog.createTable(tablePathSQL_Sql, sqlServerCatalogTable, true);
        postgresCatalog.createTable(tablePathPG, sqlServerCatalogTable, true);
        //        oracleCatalog.createTable(tablePathOracle, sqlServerCatalogTable, true);
        mySqlCatalog.createTable(tablePathMySql, sqlServerCatalogTable, true);

        Assertions.assertTrue(checkMysql(mysqlCheck));
        //        Assertions.assertTrue(checkOracle(oracleCheck));
        Assertions.assertTrue(checkSqlServer(sqlserverCheck));
        Assertions.assertTrue(checkPG(pgCheck));

        // delete table
        log.info("delete table");
        sqlServerCatalog.dropTable(tablePathSQL_Sql, true);
        sqlServerCatalog.dropTable(tablePathSQL, true);
        postgresCatalog.dropTable(tablePathPG, true);
        //        oracleCatalog.dropTable(tablePathOracle, true);
        mySqlCatalog.dropTable(tablePathMySql, true);

        sqlServerCatalog.close();
        mySqlCatalog.close();
        postgresCatalog.close();
    }

    @Override
    public void tearDown() throws Exception {
        if (sqlserver_container != null) {
            sqlserver_container.close();
        }
        if (mysql_container != null) {
            mysql_container.close();
        }
        if (oracle_container != null) {
            oracle_container.close();
        }
        if (POSTGRESQL_CONTAINER != null) {
            POSTGRESQL_CONTAINER.close();
        }
    }

    private Connection getJdbcSqlServerConnection() throws SQLException {
        return DriverManager.getConnection(
                sqlserver_container.getJdbcUrl(),
                sqlserver_container.getUsername(),
                sqlserver_container.getPassword());
    }

    private Connection getJdbcMySqlConnection() throws SQLException {
        return DriverManager.getConnection(
                mysql_container.getJdbcUrl(),
                mysql_container.getUsername(),
                mysql_container.getPassword());
    }

    private Connection getJdbcPgConnection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRESQL_CONTAINER.getJdbcUrl(),
                POSTGRESQL_CONTAINER.getUsername(),
                POSTGRESQL_CONTAINER.getPassword());
    }

    private Connection getJdbcOracleConnection() throws SQLException {
        return DriverManager.getConnection(
                oracle_container.getJdbcUrl(),
                oracle_container.getUsername(),
                oracle_container.getPassword());
    }

    private void initializeJdbcTable() {
        try (Connection connection = getJdbcSqlServerConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(CREATE_DATABASE);
            statement.execute(CREATE_TABLE_SQL);
            statement.execute(getInsertSql);
            //            statement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Initializing PostgreSql table failed!", e);
        }
    }

    private boolean checkMysql(String sql) {
        try (Connection connection = getJdbcMySqlConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            boolean tableExists = false;
            if (resultSet.next()) {
                tableExists = resultSet.getBoolean(1);
            }
            return tableExists;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean checkPG(String sql) {
        try (Connection connection = getJdbcPgConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            boolean tableExists = false;
            if (resultSet.next()) {
                tableExists = resultSet.getBoolean(1);
            }
            return tableExists;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean checkSqlServer(String sql) {
        try (Connection connection = getJdbcSqlServerConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            boolean tableExists = false;
            if (resultSet.next()) {
                tableExists = resultSet.getInt(1) == 1;
            }
            return tableExists;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean checkOracle(String sql) {
        try (Connection connection = getJdbcOracleConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            boolean tableExists = false;
            if (resultSet.next()) {
                tableExists = resultSet.getInt(1) == 1;
            }
            return tableExists;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
