using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Abstractions.Data;
using System.IO;
using Rhino.Mocks.Constraints;
using Xunit;
using Raven.Client.Extensions;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Client.Indexes;
using Xunit.Extensions;
using System.Threading;

namespace Raven.Tests.Issues
{
    public class DbRestoreWithParameters : RavenTest
    {
        private readonly string voronDataDir;
        private readonly string voronDatabaseLocation;
        private readonly string voronIncrDatabaseLocation;
        private readonly string voronDatabaseName;
        private readonly string voronIncrJournalLocation;
        private readonly string voronJournalLocation;
        private readonly string voronIncrIndexesLocation;
        private readonly string voronIndexesLocation;
        private readonly string voronBackupDir; // is RestoreLocation
        private readonly string voronIncrBackupDir; // is RestoreLocation
      
        private readonly string esentDataDir;
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
        private void Add1DataAndCreateIncrementalBackup(EmbeddableDocumentStore store)
        {

            var indexDefinitionsFolder = Path.Combine(store.DocumentDatabase.Configuration.DataDirectory, "IndexDefinitions");
            if (!Directory.Exists(indexDefinitionsFolder))
                Directory.CreateDirectory(indexDefinitionsFolder);

            AddUser(store, "Fitzchak");
            AddUser(store, "Oren");
            AddUser(store, "Michael");
            AddUser(store, "Regina");
            AddUser(store, "Maxim");
            AddUser(store, "Grisha");
            Assert.DoesNotThrow(() => store.DocumentDatabase.StartBackup(voronIncrBackupDir, true, new DatabaseDocument()));
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
         //  	configuration.Settings["Raven/Voron/AllowIncrementalBackups"] = "true";
           configuration.Settings["Raven/Voron/AllowIncrementalBackups"] = "false";
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
            voronIncrBackupDir = @"C:\NewIncDBBckp\Test";
            voronDatabaseLocation = @"C:\FullRestore\Databases\DbRestoreTest";
            voronIncrDatabaseLocation = @"C:\IncFullRestore\Databases\DbRestoreTest";
            voronDatabaseName = "DbRestoreTest";
            voronIncrJournalLocation = @"D:\IncFullJournalsRestore\Test";
            voronJournalLocation = @"D:\FullJournalsRestore\Test";
            voronIndexesLocation = @"D:\FullIndexesRestore\Test";
            voronIncrIndexesLocation = @"D:\IncFullIndexesRestore\Test";

            voronDataDir = @"C:\TempDataDir";

            esentBackupDir = @"D:\ENewDbBackup\Test";
            esentIncrBackupDir = @"C:\ENewIncDBBckp\Test";
            esentDatabaseLocation = @"C:\EFullRestore\Databases\DbRestoreTest";
            esentIncrDatabaseLocation = @"C:\EIncFullRestore\Databases\DbRestoreTest";
            esentDatabaseName = "EDbRestoreTest";
            esentIncrJournalLocation = @"D:\EIncFullJournalsRestore\Test";
            esentJournalLocation = @"D:\EFullJournalsRestore\Test";
            esentIndexesLocation = @"D:\EFullIndexesRestore\Test";
            esentIncrIndexesLocation = @"D:\EIncFullIndexesRestore\Test";

            esentDataDir = @"C:\ETempDataDir";

          
          
        }

        public void InitDb(bool isVoron = true)
        {

            if (isVoron)
            {
                dbVoron = new DocumentDatabase(new RavenConfiguration
                {
                    DatabaseName = voronDatabaseName,
                    RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false
                });
            }
               
            else
            {
                dbEsent = new DocumentDatabase(new RavenConfiguration
                {
                    DatabaseName = esentDatabaseName,
                    RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
                    
                });
               
            }
           
        }
        public IEnumerable<User> ReadIncrRestore()
        {
            using (var store = new EmbeddableDocumentStore
            {
                DataDirectory = voronIncrJournalLocation
            })
            {
                store.Initialize();
                using (var session = store.OpenSession())
                    return session.Query<User>().ToList();
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
            string storage = "voron";
            InitDb(true);
            IOExtensions.DeleteDirectory(voronIncrBackupDir);
            IEnumerable<User> preBackupData;
            using (var store = NewDocumentStore(requestedStorage: storage, runInMemory: false))
            {
                AddDataAndCreateIncrementalBackup(store,voronIncrBackupDir);
                using (var session = store.OpenSession())
                    preBackupData = session.Query<User>().ToList();
            }            
                  if (Directory.Exists(voronIncrDatabaseLocation))
                     IOExtensions.DeleteDirectory(voronIncrDatabaseLocation);
          

            if (Directory.Exists(voronIncrJournalLocation))
                IOExtensions.DeleteDirectory(voronIncrJournalLocation);

            if (Directory.Exists(voronIncrIndexesLocation))
                IOExtensions.DeleteDirectory(voronIncrIndexesLocation);

             DatabaseDocument databaseDocument = null;
             var restoreStatus = new RestoreStatus { Messages = new List<string>() };

             var databaseDocumentPath = Path.Combine(voronIncrBackupDir, "Database.Document");
             if (File.Exists(databaseDocumentPath))
             {
                 var databaseDocumentText = File.ReadAllText(databaseDocumentPath);
                 databaseDocument = RavenJObject.Parse(databaseDocumentText).JsonDeserialization<DatabaseDocument>();
             }

             var databaseName = !string.IsNullOrWhiteSpace(voronDatabaseName) ? voronDatabaseName
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
                 return;
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

             if (File.Exists(Path.Combine(voronBackupDir, Voron.Impl.Constants.DatabaseFilename)))
                 ravenConfiguration.DefaultStorageTypeName = typeof(Raven.Storage.Voron.TransactionalStorage).AssemblyQualifiedName;
             else if (Directory.Exists(Path.Combine(voronIncrBackupDir, "new")))
                 ravenConfiguration.DefaultStorageTypeName = typeof(Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName;

             ravenConfiguration.CustomizeValuesForTenant(databaseName);
             ravenConfiguration.Initialize();

             ravenConfiguration.DataDirectory = voronIncrDatabaseLocation;

             var defrag = true;
             var state = new RavenJObject
            {
                {"Done", false},
                {"Error", null}
            };

             DocumentDatabase.Restore(ravenConfiguration, voronIncrBackupDir, null,
                 msg =>
                 {
                     restoreStatus.Messages.Add(msg);
                 }, defrag, voronIncrIndexesLocation, voronIncrJournalLocation);

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

        private void InitializeDocumentDatabase(string storageName)
	    {
	        dbVoron = new DocumentDatabase(new RavenConfiguration
	        {
                DefaultStorageTypeName = storageName,
	            DataDirectory = voronDataDir,
                RunInMemory = false,
	            RunInUnreliableYetFastModeThatIsNotSuitableForProduction = false,
	            Settings =
	            {
	                {"Raven/Esent/CircularLog", "false"}
	            }
	        });
	        dbVoron.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
	    }

        
        
         [Fact]
        public void CheckVoronRestoreWithParams()
        {
            string storageName = "voron";
            InitDb(true);
            //??  InitializeDocumentDatabase(storageName);
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

            if (Directory.Exists(voronDatabaseLocation))
                IOExtensions.DeleteDirectory(voronDatabaseLocation);

            if (Directory.Exists(voronJournalLocation))
                IOExtensions.DeleteDirectory(voronJournalLocation);

            if (Directory.Exists(voronIndexesLocation))
                IOExtensions.DeleteDirectory(voronIndexesLocation);

            DatabaseDocument databaseDocument = null;
            var restoreStatus = new RestoreStatus { Messages = new List<string>() };

            var databaseDocumentPath = Path.Combine(voronBackupDir, "Database.Document");
            if (File.Exists(databaseDocumentPath))
            {
                var databaseDocumentText = File.ReadAllText(databaseDocumentPath);
                databaseDocument = RavenJObject.Parse(databaseDocumentText).JsonDeserialization<DatabaseDocument>();
            }

            var databaseName = !string.IsNullOrWhiteSpace(voronDatabaseName) ? voronDatabaseName
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
                return;
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

            if (File.Exists(Path.Combine(voronBackupDir, Voron.Impl.Constants.DatabaseFilename)))
                ravenConfiguration.DefaultStorageTypeName = typeof(Raven.Storage.Voron.TransactionalStorage).AssemblyQualifiedName;
            else if (Directory.Exists(Path.Combine(voronBackupDir, "new")))
                ravenConfiguration.DefaultStorageTypeName = typeof(Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName;

            ravenConfiguration.CustomizeValuesForTenant(databaseName);
            ravenConfiguration.Initialize();

            string documentDataDir;
            ravenConfiguration.DataDirectory = voronDatabaseLocation;

            var defrag = true;
            var state = new RavenJObject
            {
                {"Done", false},
                {"Error", null}
            };

            DocumentDatabase.Restore(ravenConfiguration, voronBackupDir, null,
                msg =>
                {
                    restoreStatus.Messages.Add(msg);
                }, defrag, voronIndexesLocation, voronJournalLocation);

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
             string storage = "esent";
             InitDb(true);
             IOExtensions.DeleteDirectory(esentIncrBackupDir);
             IEnumerable<User> preBackupData;
             using (var store = NewDocumentStore(requestedStorage: storage, runInMemory: false))
             {
                 AddDataAndCreateIncrementalBackup(store, esentIncrBackupDir);
                 using (var session = store.OpenSession())
                     preBackupData = session.Query<User>().ToList();
             }
             if (Directory.Exists(esentIncrDatabaseLocation))
                 IOExtensions.DeleteDirectory(esentIncrDatabaseLocation);


             if (Directory.Exists(esentIncrJournalLocation))
                 IOExtensions.DeleteDirectory(esentIncrJournalLocation);

             if (Directory.Exists(esentIncrIndexesLocation))
                 IOExtensions.DeleteDirectory(esentIncrIndexesLocation);

             DatabaseDocument databaseDocument = null;
             var restoreStatus = new RestoreStatus { Messages = new List<string>() };

             var databaseDocumentPath = Path.Combine(esentIncrBackupDir, "Database.Document");
             if (File.Exists(databaseDocumentPath))
             {
                 var databaseDocumentText = File.ReadAllText(databaseDocumentPath);
                 databaseDocument = RavenJObject.Parse(databaseDocumentText).JsonDeserialization<DatabaseDocument>();
             }

             var databaseName = !string.IsNullOrWhiteSpace(esentDatabaseName) ? esentDatabaseName
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
                 return;
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

  
             ravenConfiguration.DefaultStorageTypeName = typeof(Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName;

             ravenConfiguration.CustomizeValuesForTenant(databaseName);
             ravenConfiguration.Initialize();

             string documentDataDir;
             ravenConfiguration.DataDirectory = esentIncrDatabaseLocation;

             var defrag = true;
             var state = new RavenJObject
            {
                {"Done", false},
                {"Error", null}
            };

             DocumentDatabase.Restore(ravenConfiguration, esentIncrBackupDir, null,
                 msg =>
                 {
                     restoreStatus.Messages.Add(msg);
                 }, defrag, esentIncrIndexesLocation, esentIncrJournalLocation);

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
             //??  InitializeDocumentDatabase(storageName);
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

             if (Directory.Exists(esentDatabaseLocation))
                 IOExtensions.DeleteDirectory(esentDatabaseLocation);

             if (Directory.Exists(esentJournalLocation))
                 IOExtensions.DeleteDirectory(esentJournalLocation);

             if (Directory.Exists(esentIndexesLocation))
                 IOExtensions.DeleteDirectory(esentIndexesLocation);

             DatabaseDocument databaseDocument = null;
             var restoreStatus = new RestoreStatus { Messages = new List<string>() };

             var databaseDocumentPath = Path.Combine(esentBackupDir, "Database.Document");
             if (File.Exists(databaseDocumentPath))
             {
                 var databaseDocumentText = File.ReadAllText(databaseDocumentPath);
                 databaseDocument = RavenJObject.Parse(databaseDocumentText).JsonDeserialization<DatabaseDocument>();
             }

             var databaseName = !string.IsNullOrWhiteSpace(esentDatabaseName) ? esentDatabaseName
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
                 return;
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

  
             ravenConfiguration.DefaultStorageTypeName = typeof(Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName;

             ravenConfiguration.CustomizeValuesForTenant(databaseName);
             ravenConfiguration.Initialize();

             string documentDataDir;
             ravenConfiguration.DataDirectory = esentDatabaseLocation;

             var defrag = true;
             var state = new RavenJObject
            {
                {"Done", false},
                {"Error", null}
            };

             DocumentDatabase.Restore(ravenConfiguration, esentBackupDir, null,
                 msg =>
                 {
                     restoreStatus.Messages.Add(msg);
                 }, defrag, esentIndexesLocation, esentJournalLocation);

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


