﻿using System;
using System.IO;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Extensions;

namespace Raven.Database.Storage
{
    public abstract class BaseRestoreOperation
    {
	    private const string IndexesSubfolder = "Indexes";
	    protected static readonly ILog log = LogManager.GetCurrentClassLogger();

        protected readonly Action<string> output;

        protected readonly string backupLocation;
        protected  string CustomIndexesLocation { get; set; }
        protected string JournalLocation { get; set; }

        protected readonly InMemoryRavenConfiguration configuration;
        protected string databaseLocation { get { return configuration.DataDirectory.ToFullPath(); } }
        protected string indexLocation { get { return configuration.IndexStoragePath.ToFullPath(); } }

        protected BaseRestoreOperation(string backupLocation, InMemoryRavenConfiguration configuration, Action<string> output, string customIndexesLocation=null, string journalLocation=null)
        {
            this.backupLocation = backupLocation;
            this.configuration = configuration;
            this.output = output;
            this.CustomIndexesLocation = string.IsNullOrEmpty(customIndexesLocation) ? indexLocation : customIndexesLocation;
            this.JournalLocation = string.IsNullOrEmpty(journalLocation) ? databaseLocation : journalLocation;


        }

        public abstract void Execute();

        protected void LogFailureAndRethrow(Exception e)
        {
            output("Restore Operation: Failure! Could not restore database!");
            output(e.ToString());
            log.WarnException("Could not complete restore", e);

            throw e;
        }

        protected string ValidateRestorePreconditions(string backupFilename)
        {
            if (File.Exists(BackupFilenamePath(backupFilename)) == false)
            {
                output("Error: " + backupLocation + " doesn't look like a valid backup");
                output("Error: Restore Canceled");
                throw new InvalidOperationException(backupLocation + " doesn't look like a valid backup");
            }

            if (Directory.Exists(databaseLocation) && Directory.GetFileSystemEntries(databaseLocation).Length > 0)
            {
                output("Error: Database already exists, cannot restore to an existing database.");
                output("Error: Restore Canceled");
                throw new IOException("Database already exists, cannot restore to an existing database.");
            }

            if (Directory.Exists(databaseLocation) == false)
                Directory.CreateDirectory(databaseLocation);

           // if (Directory.Exists(indexLocation) == false)
           //     Directory.CreateDirectory(indexLocation);
            if (Directory.Exists(CustomIndexesLocation) == false)
                Directory.CreateDirectory(CustomIndexesLocation);

            var logsPath = databaseLocation;

            if (string.IsNullOrWhiteSpace(configuration.Settings[Constants.RavenLogsPath])) return logsPath;

            logsPath = configuration.Settings[Constants.RavenLogsPath].ToFullPath();

            if (Directory.Exists(logsPath) == false)
            {
                Directory.CreateDirectory(logsPath);
            }
            return logsPath;
        }

        protected string BackupIndexesPath()
        {
            return Path.Combine(backupLocation, "Indexes");
        }

        private void CopyAll(DirectoryInfo source, DirectoryInfo target)
        {
            // Check if the target directory exists, if not, create it.
            if (Directory.Exists(target.FullName) == false)
            {
                Directory.CreateDirectory(target.FullName);
            }

            // Copy each file into it's new directory.
            foreach (FileInfo fi in source.GetFiles())
            {
                output(string.Format(@"Copying {0}\{1}", target.FullName, fi.Name));
                Console.WriteLine(@"Copying {0}\{1}", target.FullName, fi.Name);
                fi.CopyTo(Path.Combine(target.ToString(), fi.Name), true);
            }

            // Copy each subdirectory using recursion.
            foreach (DirectoryInfo diSourceSubDir in source.GetDirectories())
            {
                DirectoryInfo nextTargetSubDir =
                    target.CreateSubdirectory(diSourceSubDir.Name);
                CopyAll(diSourceSubDir, nextTargetSubDir);
            }
        }

        private void ForceIndexReset(string indexPath, string indexName, Exception ex)
        {
            if (Directory.Exists(indexPath))
                IOExtensions.DeleteDirectory(indexPath); // this will force index reset

            output(
                string.Format(
                    "Error: Index {0} could not be restored. All already copied index files was deleted. " +
                    "Index will be recreated after launching Raven instance. Thrown exception:{1}{2}",
                    indexName, Environment.NewLine, ex));
        }

	    protected void CopyIndexDefinitions()
	    {
			var directories = Directory.GetDirectories(backupLocation, "Inc*")
												  .OrderByDescending(dir => dir)
												  .ToList();

			string indexDefinitionsBackupFolder;
			string indexDefinitionsDestinationFolder = Path.Combine(databaseLocation, "IndexDefinitions");
			if (directories.Count == 0)
			    indexDefinitionsBackupFolder = Path.Combine(backupLocation, "IndexDefinitions");
		    else
		    {
				var latestIncrementalBackupDirectory = directories.First();
			    if (Directory.Exists(Path.Combine(latestIncrementalBackupDirectory, "IndexDefinitions")) == false)
			    {
				    output("Failed to restore index definitions. It seems the index definitions are missing from backup folder.");
					return;
			    }
				indexDefinitionsBackupFolder = Path.Combine(latestIncrementalBackupDirectory, "IndexDefinitions");
		    }

			try
			{
				CopyAll(new DirectoryInfo(indexDefinitionsBackupFolder), new DirectoryInfo(indexDefinitionsDestinationFolder));
			}
			catch (Exception ex)
			{
				output("Failed to restore index definitions. This is not supposed to happen. Reason : " + ex);
			}
		}

        protected void CopyIndexes()
        {
            var directories = Directory.GetDirectories(backupLocation, "Inc*")
                                       .OrderByDescending(dir => dir)
                                       .ToList();

	        if (directories.Count == 0)
            {
                foreach (var backupIndex in Directory.GetDirectories(Path.Combine(backupLocation, IndexesSubfolder)))
                {
                    var indexName = Path.GetFileName(backupIndex);
                  //!!  var indexPath = Path.Combine(indexLocation, indexName);
                    var indexPath = Path.Combine(CustomIndexesLocation, indexName);

                    try
                    {
                        CopyAll(new DirectoryInfo(backupIndex), new DirectoryInfo(indexPath));
                    }
                    catch (Exception ex)
                    {
						output("Failed to restore indexes, forcing index reset. Reason : " + ex);
						ForceIndexReset(indexPath, indexName, ex);
                    }
                }

                return;
            }

            var latestIncrementalBackupDirectory = directories.First();
            if (Directory.Exists(Path.Combine(latestIncrementalBackupDirectory, IndexesSubfolder)) == false)
                return;

            directories.Add(backupLocation); // add the root (first full backup) to the end of the list (last place to look for)

            foreach (var index in Directory.GetDirectories(Path.Combine(latestIncrementalBackupDirectory, IndexesSubfolder)))
            {
                var indexName = Path.GetFileName(index);
               // var indexPath = Path.Combine(indexLocation, indexName);
                var indexPath = Path.Combine(CustomIndexesLocation, indexName);

                try
                {
                    var filesList = File.ReadAllLines(Path.Combine(index, "index-files.required-for-index-restore"))
                        .Where(x => string.IsNullOrEmpty(x) == false)
                        .Reverse();

                    output("Copying Index: " + indexName);

                    if (Directory.Exists(indexPath) == false)
                        Directory.CreateDirectory(indexPath);

                    foreach (var neededFile in filesList)
                    {
                        var found = false;

                        foreach (var directory in directories)
                        {
                            var possiblePathToFile = Path.Combine(directory,IndexesSubfolder , indexName, neededFile);
                            if (File.Exists(possiblePathToFile) == false)
                                continue;

                            found = true;
                            File.Copy(possiblePathToFile, Path.Combine(indexPath, neededFile));
                            break;
                        }

                        if (found == false)
                            output(string.Format("Error: File \"{0}\" is missing from index {1}", neededFile, indexName));
                    }
                }
                catch (Exception ex)
                {
                    ForceIndexReset(indexPath, indexName, ex);
                }
            }
        }

        protected string BackupFilenamePath(string backupFilename)
        {
            var directories = Directory.GetDirectories(backupLocation, "Inc*")
                .OrderByDescending(dir => dir)
                .ToList();

            var backupFilenamePath = Path.Combine(directories.Count == 0 ? backupLocation : directories.First(), backupFilename);
            return backupFilenamePath;
        }
    }
}
