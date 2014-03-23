using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Database;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Abstractions.Data;
using System.IO;
using Xunit;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Client.Indexes;
using System.Threading;

namespace Raven.Tests.Issues
{
    public class DbRestoreWithParameters : RavenTest
    {
        private readonly string voronDatabaseLocation;
        private readonly string voronIncrDatabaseLocation;
        private readonly string voronDatabaseName;
        private readonly string voronIncrJournalLocation;
        private readonly string voronJournalLocation;
        private readonly string voronIncrIndexesLocation;
        private readonly string voronIndexesLocation;
        private readonly string voronBackupDir; // is RestoreLocation
        private readonly string voronIncrBackupDir; // is RestoreLocation
      
        private readonly string esentDatabaseLocation;
        private readonly string esentIncrDatabaseLocation;
        private readonly string esentDatabaseName;
        private readonly string esentIncrJournalLocation;
        private readonly string esentJournalLocation;
        private readonly string esentIncrIndexesLocation;
        private readonly string esentIndexesLocation;
        private readonly string esentBackupDir; // is RestoreLocation
        private readonly string esentIncrBackupDir; // is RestoreLocation

        private DocumentDatabase dbVoron;
        private DocumentDatabase dbEsent;


        public class User
		{
			public string Name { get; set; }
		}




        private void AddDataAndCreateIncrementalBackup(EmbeddableDocumentStore store, string incrBackupDir)
        {

            var indexDefinitionsFolder = Path.Combine(store.DocumentDatabase.Configuration.DataDirectory, "IndexDefinitions");
            if (!Directory.Exists(indexDefinitionsFolder))
                Directory.CreateDirectory(indexDefinitionsFolder);

            AddUser(store, "Fitzchak");
            Assert.DoesNotThrow(() => store.DocumentDatabase.StartBackup(incrBackupDir, true, new DatabaseDocument()));
            WaitForBackup(store.DocumentDatabase, true);
            Thread.Sleep(2000);


            AddUser(store, "Oren");
            Assert.DoesNotThrow(() => store.DocumentDatabase.StartBackup(incrBackupDir, true, new DatabaseDocument()));
            WaitForBackup(store.DocumentDatabase, true);
            Thread.Sleep(2000);

            AddUser(store, "Regina");
            Assert.DoesNotThrow(() => store.DocumentDatabase.StartBackup(incrBackupDir, true, new DatabaseDocument()));
            WaitForBackup(store.DocumentDatabase, true);
            Thread.Sleep(2000);

            AddUser(store, "Michael");
            Assert.DoesNotThrow(() => store.DocumentDatabase.StartBackup(incrBackupDir, true, new DatabaseDocument()));
            WaitForBackup(store.DocumentDatabase, true);
            Thread.Sleep(2000);

            AddUser(store, "Maxim");
            Assert.DoesNotThrow(() => store.DocumentDatabase.StartBackup(incrBackupDir, true, new DatabaseDocument()));
            WaitForBackup(store.DocumentDatabase, true);
            Thread.Sleep(2000);

        }
     

        private static void AddUser(EmbeddableDocumentStore store, string usename)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User {Name = usename});
                session.SaveChanges();
            }
        }

        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/Esent/CircularLog"] = "false";
           	configuration.Settings["Raven/Voron/AllowIncrementalBackups"] = "true";
         //!!  configuration.Settings["Raven/Voron/AllowIncrementalBackups"] = "false";
            configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false;
        }

        public override void Dispose()
        {
            if(dbVoron!=null)
            dbVoron.Dispose();
            if(dbEsent!=null)
                dbEsent.Dispose();
            base.Dispose();
        }

        
        public DbRestoreWithParameters()
        {
            voronBackupDir = @"D:\NewDbBackup\Test";
            voronIncrBackupDir = @"D:\NewIncDBBckp\Test";
            voronDatabaseLocation = @"C:\FullRestore\Databases\DbRestoreTest";
            voronIncrDatabaseLocation = @"C:\IncFullRestore\Databases\DbRestoreTest";
            voronDatabaseName = "DbRestoreTest";
            voronIncrJournalLocation = @"D:\IncFullJournalsRestore\Test";
            voronJournalLocation = @"D:\FullJournalsRestore\Test";
            voronIndexesLocation = @"D:\FullIndexesRestore\Test";
            voronIncrIndexesLocation = @"D:\IncFullIndexesRestore\Test";

            esentBackupDir = @"D:\ENewDbBackup\Test";
            esentIncrBackupDir = @"C:\ENewIncDBBckp\Test";
            esentDatabaseLocation = @"C:\EFullRestore\Databases\DbRestoreTest";
            esentIncrDatabaseLocation = @"C:\EIncFullRestore\Databases\DbRestoreTest";
            esentDatabaseName = "EDbRestoreTest";
            esentIncrJournalLocation = @"D:\EIncFullJournalsRestore\Test";
            esentJournalLocation = @"D:\EFullJournalsRestore\Test";
            esentIndexesLocation = @"D:\EFullIndexesRestore\Test";
            esentIncrIndexesLocation = @"D:\EIncFullIndexesRestore\Test";
       }

        public void InitDb(bool isVoron = true)
        {

            if (isVoron)
            {
                dbVoron = new DocumentDatabase(new RavenConfiguration
                {
                    DatabaseName = voronDatabaseName,
                    RunInMemory = false,
                    RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false
                });
            }
               
            else
            {
                dbEsent = new DocumentDatabase(new RavenConfiguration
                {
                    DatabaseName = esentDatabaseName,
                    RunInMemory = false,
                    RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
                    
                });
               
            }
           
        }
         public IEnumerable<User> ReadRestore(string journalLocation)
        {

            using (var store = new EmbeddableDocumentStore()
            {
                DataDirectory = journalLocation
            })
            {
                store.Initialize();
                using (var session = store.OpenSession())
                    return session.Query<User>().ToList();
            }

        }
        [Fact]
         public void IncrementalVoronRestoreWithParams()
        {
            const string storage = "voron";
            InitDb();
            IOExtensions.DeleteDirectory(voronIncrBackupDir);
            IEnumerable<User> preBackupData;
            using (var store = NewDocumentStore(requestedStorage: storage, runInMemory: false))
            {
                AddDataAndCreateIncrementalBackup(store,voronIncrBackupDir);
                using (var session = store.OpenSession())
                    preBackupData = session.Query<User>().ToList();
            }

            DeleteDirectories(voronIncrDatabaseLocation, voronIncrJournalLocation, voronIncrIndexesLocation);

            if (!RestoreDatabase(voronIncrBackupDir, voronDatabaseName, voronIncrDatabaseLocation, true,voronIncrIndexesLocation, voronIncrJournalLocation))
            {

                return ;
            }

            bool dirExists = (Directory.Exists(voronIncrJournalLocation)) && (Directory.Exists(voronIncrIndexesLocation)) && (Directory.Exists(voronIncrDatabaseLocation));
            Assert.Equal(dirExists, true);
            var afterRestoreData = ReadRestore(voronIncrJournalLocation).ToList();


            Assert.Equal(true, preBackupData.Count() == afterRestoreData.Count());

            var preBackupDataNames = preBackupData.ToList().Select(source => source.Name).ToList();
            var afterRestoreDataNames = afterRestoreData.ToList().Select(source => source.Name).ToList();

            foreach (var preBackupDataName in preBackupDataNames)
            {
                Assert.Equal(true, afterRestoreDataNames.Contains(preBackupDataName));

            }
  
        }

        private static void DeleteDirectories(string dbLocation,string journalLocation,string indexesLocation)
        {
            if (Directory.Exists(dbLocation))
                IOExtensions.DeleteDirectory(dbLocation);


            if (Directory.Exists(journalLocation))
                IOExtensions.DeleteDirectory(journalLocation);

            if (Directory.Exists(indexesLocation))
                IOExtensions.DeleteDirectory(indexesLocation);
        }

        private static bool RestoreDatabase(string backupDir,string dbName, string databaseLocation,bool isVoronType,string indexesLocation=null, string journalLocation=null)
        {
            DatabaseDocument databaseDocument = null;
            var restoreStatus = new RestoreStatus {Messages = new List<string>()};

            var databaseDocumentPath = Path.Combine(backupDir, "Database.Document");
            if (File.Exists(databaseDocumentPath))
            {
                var databaseDocumentText = File.ReadAllText(databaseDocumentPath);
                databaseDocument = RavenJObject.Parse(databaseDocumentText).JsonDeserialization<DatabaseDocument>();
            }

            var databaseName = !string.IsNullOrWhiteSpace(dbName) ? dbName
                : databaseDocument == null ? null : databaseDocument.Id;

            if (string.IsNullOrWhiteSpace(databaseName))
            {
                var errorMessage = (databaseDocument == null || String.IsNullOrWhiteSpace(databaseDocument.Id))
                    ? "Database.Document file is invalid - database name was not found and not supplied in the request (Id property is missing or null). This is probably a bug - should never happen."
                    : "A database name must be supplied if the restore location does not contain a valid Database.Document file";
            }

            if (databaseName == Constants.SystemDatabase)
            {
                Console.WriteLine("Cannot do an online restore for the <system> database");
                return false;
            }


            var ravenConfiguration = new RavenConfiguration
            {
                DatabaseName = databaseName,
            };

            if (databaseDocument != null)
            {
                foreach (var setting in databaseDocument.Settings)
                {
                    ravenConfiguration.Settings[setting.Key] = setting.Value;
                }
            }

            ravenConfiguration.DefaultStorageTypeName = isVoronType ? typeof (Raven.Storage.Voron.TransactionalStorage).AssemblyQualifiedName : typeof (Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName;

            ravenConfiguration.CustomizeValuesForTenant(databaseName);
            ravenConfiguration.Initialize();

            ravenConfiguration.DataDirectory = databaseLocation;

            var defrag = true;
            var state = new RavenJObject
            {
                {"Done", false},
                {"Error", null}
            };

            DocumentDatabase.Restore(ravenConfiguration, backupDir, null,
                msg => { restoreStatus.Messages.Add(msg); }, defrag, indexesLocation, journalLocation);
            return true;
        }

      
        
        
         [Fact]
        public void CheckVoronRestoreWithParams()
        {
            string storageName = "voron";
            InitDb(true);
            IOExtensions.DeleteDirectory(voronBackupDir);

            IEnumerable<User> preBackupData;

            using (var store = NewDocumentStore(requestedStorage: "voron", runInMemory: false))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" });
                    session.Store(new User { Name = "Michael" });
                    session.Store(new User { Name = "Maxim" });
                    session.Store(new User { Name = "Grisha" });
                    session.SaveChanges();

                    preBackupData = session.Query<User>()
                                           .Customize(x => x.WaitForNonStaleResults())
                                           .ToList();
                }
                //!! good 
                Assert.DoesNotThrow(() => store.DocumentDatabase.StartBackup(voronBackupDir, false, new DatabaseDocument()));
                WaitForBackup(store.DocumentDatabase, true);

            }

            //!! check what's wrong
            //??  dbVoron.StartBackup(voronBackupDir, false, new DatabaseDocument());
            //??  WaitForBackup(dbVoron, true);

            dbVoron.Dispose();

            DeleteDirectories(voronDatabaseLocation, voronJournalLocation, voronIndexesLocation);

   
            if (!RestoreDatabase(voronBackupDir, voronDatabaseName, voronDatabaseLocation, true,voronIndexesLocation, voronJournalLocation))
            {

                return;
            }

            bool dirExists = (Directory.Exists(voronJournalLocation)) && (Directory.Exists(voronIndexesLocation)) && (Directory.Exists(voronDatabaseLocation));
            Assert.Equal(dirExists, true);



            var afterRestoreData = ReadRestore(voronJournalLocation).ToList();

            Assert.Equal(true, preBackupData.Count() == afterRestoreData.Count());

            var preBackupDataNames = preBackupData.ToList().Select(source => source.Name).ToList();
            var afterRestoreDataNames = afterRestoreData.ToList().Select(source => source.Name).ToList();

            foreach (var preBackupDataName in preBackupDataNames)
            {
                Assert.Equal(true, afterRestoreDataNames.Contains(preBackupDataName));

            }


        }

         [Fact]
         public void IncrementalEsentRestoreWithParams()
         {
             const string storage = "esent";
             InitDb(true);
             IOExtensions.DeleteDirectory(esentIncrBackupDir);
             IEnumerable<User> preBackupData;
             using (var store = NewDocumentStore(requestedStorage: storage, runInMemory: false))
             {
                 AddDataAndCreateIncrementalBackup(store, esentIncrBackupDir);
                 using (var session = store.OpenSession())
                     preBackupData = session.Query<User>().ToList();
             }

             DeleteDirectories(esentIncrDatabaseLocation, esentIncrJournalLocation, esentIncrIndexesLocation);
    
             if (!RestoreDatabase(esentIncrBackupDir, esentDatabaseName, esentIncrDatabaseLocation, false, esentIncrIndexesLocation, esentIncrJournalLocation))
             {

                 return;
             }

             bool dirExists = (Directory.Exists(esentIncrJournalLocation)) && (Directory.Exists(esentIncrIndexesLocation)) && (Directory.Exists(esentIncrDatabaseLocation));
             Assert.Equal(dirExists, true);

             var afterRestoreData = ReadRestore(esentIncrJournalLocation).ToList();

             Assert.Equal(true, preBackupData.Count() == afterRestoreData.Count());

             var preBackupDataNames = preBackupData.ToList().Select(source => source.Name).ToList();
             var afterRestoreDataNames = afterRestoreData.ToList().Select(source => source.Name).ToList();

             foreach (var preBackupDataName in preBackupDataNames)
             {
                 Assert.Equal(true, afterRestoreDataNames.Contains(preBackupDataName));

             }

    
         }

        [Fact]
         public void CheckEsentRestoreWithParams()
         {
             const string storageName = "esent";
             InitDb(false);
             IOExtensions.DeleteDirectory(esentBackupDir);

             IEnumerable<User> preBackupData;

             using (var store = NewDocumentStore(requestedStorage: storageName, runInMemory: true))
             {
                
                 using (var session = store.OpenSession())
                 {
                     session.Store(new User { Name = "Oren" });
                     session.Store(new User { Name = "Michael" });
                     session.Store(new User { Name = "Maxim" });
                     session.Store(new User { Name = "Grisha" });
                     session.SaveChanges();

                     preBackupData = session.Query<User>()
                                            .Customize(x => x.WaitForNonStaleResults())
                                            .ToList();
                 }
                 //!! good 
                 Assert.DoesNotThrow(() => store.DocumentDatabase.StartBackup(esentBackupDir, false, new DatabaseDocument()));
                 WaitForBackup(store.DocumentDatabase, true);

             }

             //!! check what's wrong
             //??  dbVoron.StartBackup(voronBackupDir, false, new DatabaseDocument());
             //??  WaitForBackup(dbVoron, true);

             dbEsent.Dispose();
             DeleteDirectories(esentDatabaseLocation, esentJournalLocation, esentIndexesLocation);

             if (!RestoreDatabase(esentBackupDir, esentDatabaseName, esentDatabaseLocation, false, esentIndexesLocation, esentJournalLocation))
             {

                 return;
             }
             bool dirExists = (Directory.Exists(esentJournalLocation)) && (Directory.Exists(esentIndexesLocation)) && (Directory.Exists(esentDatabaseLocation));
             Assert.Equal(dirExists, true);



             var afterRestoreData = ReadRestore(esentJournalLocation).ToList();

             Assert.Equal(true, preBackupData.Count() == afterRestoreData.Count());

             var preBackupDataNames = preBackupData.ToList().Select(source => source.Name).ToList();
             var afterRestoreDataNames = afterRestoreData.ToList().Select(source => source.Name).ToList();

             foreach (var preBackupDataName in preBackupDataNames)
             {
                 Assert.Equal(true, afterRestoreDataNames.Contains(preBackupDataName));

             }


         }

       
    }
}


