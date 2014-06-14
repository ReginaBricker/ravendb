﻿using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Reactive.Linq;
using System.Threading.Tasks;
using NLog;
using Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Util;
using Raven.Database.Config;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Notifications;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Storage.Esent;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;
using Raven.Database.Server.RavenFS.Util;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;
using Raven.Client.FileSystem;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Client.FileSystem.Connection;
using System.Collections.Concurrent;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class SynchronizationTask
	{
		private const int DefaultLimitOfConcurrentSynchronizations = 5;

		private static readonly Logger Log = LogManager.GetCurrentClassLogger();

		private readonly NotificationPublisher publisher;
		private readonly ITransactionalStorage storage;
		private readonly SynchronizationQueue synchronizationQueue;
		private readonly SynchronizationStrategy synchronizationStrategy;
		private readonly InMemoryRavenConfiguration systemConfiguration;

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, SynchronizationDetails>> activeIncomingSynchronizations =
            new ConcurrentDictionary<string, ConcurrentDictionary<string, SynchronizationDetails>>();

		private readonly IObservable<long> timer = Observable.Interval(TimeSpan.FromMinutes(10));
		private int failedAttemptsToGetDestinationsConfig;

		public SynchronizationTask(ITransactionalStorage storage, SigGenerator sigGenerator, NotificationPublisher publisher,
								   InMemoryRavenConfiguration systemConfiguration)
		{
			this.storage = storage;
			this.publisher = publisher;
			this.systemConfiguration = systemConfiguration;
			synchronizationQueue = new SynchronizationQueue();
			synchronizationStrategy = new SynchronizationStrategy(storage, sigGenerator);

		    LastSuccessfulSynchronizationTime = DateTime.MinValue;

			InitializeTimer();
		}

        public DateTime LastSuccessfulSynchronizationTime { get; private set; }

		public string FileSystemUrl
		{
			get { return string.Format("{0}/fs/{1}", systemConfiguration.ServerUrl.TrimEnd('/'), systemConfiguration.FileSystemName); }
		}

		public SynchronizationQueue Queue
		{
			get { return synchronizationQueue; }
		}

        public void IncomingSynchronizationStarted(string fileName, ServerInfo sourceServerInfo, Guid sourceFileETag, SynchronizationType type)
        {
            var activeForDestination = activeIncomingSynchronizations.GetOrAdd(sourceServerInfo.FileSystemUrl,
                                                       new ConcurrentDictionary<string, SynchronizationDetails>());

            var syncDetails = new SynchronizationDetails()
            {
                DestinationUrl = sourceServerInfo.FileSystemUrl,
                FileETag = sourceFileETag,
                FileName = fileName,
                Type = type
            };
            if (activeForDestination.TryAdd(fileName, syncDetails))
            {
                Log.Debug("File '{0}' with ETag {1} was added to an incomign active synchronization queue for a destination {2}",
                          fileName,
                          sourceFileETag, sourceServerInfo.FileSystemUrl);
            }
        }

        public void IncomingSynchronizationFinished(string fileName, ServerInfo sourceServerInfo, Guid sourceFileETag)
        {
            ConcurrentDictionary<string, SynchronizationDetails> activeSourceTasks;

            if (activeIncomingSynchronizations.TryGetValue(sourceServerInfo.FileSystemUrl, out activeSourceTasks) == false)
            {
                Log.Warn("Could not get an active synchronization queue for {0}", sourceServerInfo.FileSystemUrl);
                return;
            }

            SynchronizationDetails removingItem;
            if (activeSourceTasks.TryRemove(fileName, out removingItem))
            {
                Log.Debug("File '{0}' with ETag {1} was removed from an active synchronization queue for a destination {2}",
                          fileName, sourceFileETag, sourceServerInfo);
            }
        }

        public IEnumerable<SynchronizationDetails> IncomingQueue
        {
            get
            {
                return from destinationActive in activeIncomingSynchronizations
                       from activeFile in destinationActive.Value
                       select activeFile.Value;
            }
        }

		private void InitializeTimer()
		{
			timer.Subscribe(tick => SynchronizeDestinationsAsync());
		}

        public Task<DestinationSyncResult> SynchronizeDestinationAsync(string filesystemDestination, bool forceSyncingContinuation = true)
        {
            foreach (var destination in GetSynchronizationDestinations())
            {
                if (string.Compare(filesystemDestination, destination.FileSystemUrl, StringComparison.OrdinalIgnoreCase) == 0)
                {
                    Log.Debug("Starting to synchronize a destination server {0}", destination.FileSystemUrl);

                    if (!CanSynchronizeTo(destination.FileSystemUrl))
                    {
                        Log.Debug("Could not synchronize to {0} because no synchronization request was available", destination.FileSystemUrl);

                        return (Task<DestinationSyncResult>)Task<DestinationSyncResult>.Run(() => 
                            { 
                                throw new SynchronizationException(
                                    string.Format("No synchronization request was available for filesystem '{0}'", destination.FileSystem)); 
                            });
                    }
                    
                    return SynchronizeDestinationAsync(destination, forceSyncingContinuation);
                }
            }

            return (Task<DestinationSyncResult>)Task<DestinationSyncResult>.Run(() => 
            {
                Log.Debug("Could not synchronize to {0} because no destination was configured for that url", filesystemDestination);
                throw new ArgumentException("Filesystem destination does not exist", "filesystemDestination"); 
            });
        }

		public Task<DestinationSyncResult[]> SynchronizeDestinationsAsync(bool forceSyncingContinuation = true)
		{
			var destinationSyncTasks = new List<Task<DestinationSyncResult>>();

			foreach (var destination in GetSynchronizationDestinations())
			{
				Log.Debug("Starting to synchronize a destination server {0}", destination.FileSystemUrl);

				if (!CanSynchronizeTo(destination.FileSystemUrl))
				{
					Log.Debug("Could not synchronize to {0} because no synchronization request was available", destination.FileSystemUrl);
					continue;
				}

				destinationSyncTasks.Add(SynchronizeDestinationAsync(destination, forceSyncingContinuation));
			}

			return Task.WhenAll(destinationSyncTasks);
		}

		public async Task<SynchronizationReport> SynchronizeFileToAsync(string fileName, SynchronizationDestination destination)
		{
            ICredentials credentials = null;
            if (string.IsNullOrEmpty(destination.Username) == false)
            {
                credentials = string.IsNullOrEmpty(destination.Domain)
                                  ? new NetworkCredential(destination.Username, destination.Password)
                                  : new NetworkCredential(destination.Username, destination.Password, destination.Domain);
            }

		    var destinationClient = new AsyncFilesServerClient(destination.ServerUrl, destination.FileSystem, apiKey: destination.ApiKey, credentials: credentials).Synchronization;

            RavenJObject destinationMetadata;

            try
            {
                destinationMetadata = await destinationClient.Commands.GetMetadataForAsync(fileName);
            }
            catch (Exception ex)
            {
                var exceptionMessage = "Could not get metadata details for " + fileName + " from " + destination.FileSystemUrl;
                Log.WarnException(exceptionMessage, ex);

                return new SynchronizationReport(fileName, Guid.Empty, SynchronizationType.Unknown)
                {
                    Exception = new SynchronizationException(exceptionMessage, ex)
                };
            }

            RavenJObject localMetadata = GetLocalMetadata(fileName);

			NoSyncReason reason;
			SynchronizationWorkItem work = synchronizationStrategy.DetermineWork(fileName, localMetadata, destinationMetadata, FileSystemUrl, out reason);

			if (work == null)
			{
				Log.Debug("File '{0}' was not synchronized to {1}. {2}", fileName, destination.FileSystemUrl, reason.GetDescription());

				return new SynchronizationReport(fileName, Guid.Empty, SynchronizationType.Unknown)
				{
					Exception = new SynchronizationException(reason.GetDescription())
				};
			}

            return await PerformSynchronizationAsync(destinationClient, work);
		}

        private async Task<DestinationSyncResult> SynchronizeDestinationAsync(SynchronizationDestination destination,
																			  bool forceSyncingContinuation)
		{
			try
			{
                ICredentials credentials = null;
                if (string.IsNullOrEmpty(destination.Username) == false)
                {
                    credentials = string.IsNullOrEmpty(destination.Domain)
                                      ? new NetworkCredential(destination.Username, destination.Password)
                                      : new NetworkCredential(destination.Username, destination.Password, destination.Domain);
                }

                var destinationClient = new AsyncFilesServerClient(destination.ServerUrl, destination.FileSystem,
                                                                  apiKey: destination.ApiKey, credentials: credentials).Synchronization;

				var lastETag = await destinationClient.GetLastSynchronizationFromAsync(storage.Id);

				var activeTasks = synchronizationQueue.Active.ToList();
				var filesNeedConfirmation = GetSyncingConfigurations(destination).Where(sync => activeTasks.All(x => x.FileName != sync.FileName)).ToList();

				var confirmations = await ConfirmPushedFiles(filesNeedConfirmation, destinationClient);

				var needSyncingAgain = new List<FileHeaderInformation>();

				foreach (var confirmation in confirmations)
				{
					if (confirmation.Status == FileStatus.Safe)
					{
						Log.Debug("Destination server {0} said that file '{1}' is safe", destination, confirmation.FileName);
						RemoveSyncingConfiguration(confirmation.FileName, destination.FileSystemUrl);
					}
					else
					{
						storage.Batch(accessor =>
						{
							var fileHeader = accessor.ReadFile(confirmation.FileName);

							if (fileHeader != null)
							{
								needSyncingAgain.Add(fileHeader);

								Log.Debug("Destination server {0} said that file '{1}' is {2}.", 
                                           destination, confirmation.FileName, confirmation.Status);
							}
						});
					}
				}

				await EnqueueMissingUpdatesAsync(destinationClient, lastETag, needSyncingAgain);

                var reports = await Task.WhenAll(SynchronizePendingFilesAsync(destinationClient, forceSyncingContinuation));

				var destinationSyncResult = new DestinationSyncResult
				{
					DestinationServer = destination.ServerUrl,
                    DestinationFileSystem = destination.FileSystem
				};

				if (reports.Length > 0)
				{
					var successfulSynchronizationsCount = reports.Count(x => x.Exception == null);

					var failedSynchronizationsCount = reports.Count(x => x.Exception != null);

					if (successfulSynchronizationsCount > 0 || failedSynchronizationsCount > 0)
					{
						Log.Debug(
							"Synchronization to a destination {0} has completed. {1} file(s) were synchronized successfully, {2} synchronization(s) were failed",
							destination.FileSystemUrl, successfulSynchronizationsCount, failedSynchronizationsCount);
					}

					destinationSyncResult.Reports = reports;
				}

				return destinationSyncResult;
			}
			catch (Exception ex)
			{
				Log.WarnException(string.Format("Failed to perform a synchronization to a destination {0}", destination), ex);

				return new DestinationSyncResult
				{
					DestinationServer = destination.ServerUrl,
                    DestinationFileSystem = destination.FileSystem,
					Exception = ex
				};
			}
		}

        private async Task EnqueueMissingUpdatesAsync(IAsyncFilesSynchronizationCommands destination,
													  SourceSynchronizationInformation lastEtag,
													  IList<FileHeaderInformation> needSyncingAgain)
		{
			LogFilesInfo("There were {0} file(s) that needed synchronization because the previous one went wrong: {1}",
						 needSyncingAgain);

            var commands = (IAsyncFilesCommandsImpl)destination.Commands;

			var filesToSynchronization = new HashSet<FileHeaderInformation>(GetFilesToSynchronization(lastEtag, 100),
																 new FileHeaderNameEqualityComparer());

			LogFilesInfo("There were {0} file(s) that needed synchronization because of greater ETag value: {1}",
						 filesToSynchronization);

			foreach (FileHeaderInformation needSyncing in needSyncingAgain)
			{
				filesToSynchronization.Add(needSyncing);
			}

			var filteredFilesToSynchronization =
				filesToSynchronization.Where(
					x => synchronizationStrategy.Filter(x, lastEtag.DestinationServerId, filesToSynchronization)).ToList();

			if (filesToSynchronization.Count > 0)
			{
				LogFilesInfo("There were {0} file(s) that needed synchronization after filtering: {1}",
							 filteredFilesToSynchronization);
			}

			if (filteredFilesToSynchronization.Count == 0)
				return;

            var baseUrl = commands.UrlFor();

			foreach (var fileHeader in filteredFilesToSynchronization)
			{
				var file = fileHeader.Name;
				var localMetadata = GetLocalMetadata(file);

				RavenJObject destinationMetadata;

                try
                {
                    destinationMetadata = await destination.Commands.GetMetadataForAsync(file);
                }
                catch (Exception ex)
                {
                    Log.WarnException(
                        string.Format("Could not retrieve a metadata of a file '{0}' from {1} in order to determine needed synchronization type", file,
                            baseUrl), ex);

                    continue;
                }                

				NoSyncReason reason;
                var work = synchronizationStrategy.DetermineWork(file, localMetadata, destinationMetadata, FileSystemUrl, out reason);
				if (work == null)
				{
                    Log.Debug("File '{0}' were not synchronized to {1}. {2}", file, baseUrl, reason.GetDescription());

					if (reason == NoSyncReason.ContainedInDestinationHistory)
					{
						var etag = localMetadata.Value<Guid>("ETag");
                        await destination.IncrementLastETagAsync(storage.Id, baseUrl, etag);
                        RemoveSyncingConfiguration(file, baseUrl);
					}

					continue;
				}

                if (synchronizationQueue.EnqueueSynchronization(baseUrl, work))
                {
                    publisher.Publish(new SynchronizationUpdateNotification
                    {
                        FileSystemName = systemConfiguration.FileSystemName,
                        FileName = work.FileName,
                        DestinationFileSystemUrl = baseUrl,
                        SourceServerId = storage.Id,
                        SourceFileSystemUrl = FileSystemUrl,
                        Type = work.SynchronizationType,
                        Action = SynchronizationAction.Enqueue,
                        SynchronizationDirection = SynchronizationDirection.Outgoing
                    });
                }
			}
		}

        private IEnumerable<Task<SynchronizationReport>> SynchronizePendingFilesAsync(IAsyncFilesSynchronizationCommands destination, bool forceSyncingContinuation)
		{
            var commands = (IAsyncFilesCommandsImpl)destination.Commands;

            var destinationUrl = commands.UrlFor();

            for (var i = 0; i < AvailableSynchronizationRequestsTo(destinationUrl); i++)
			{
				SynchronizationWorkItem work;
                if (!synchronizationQueue.TryDequePendingSynchronization(destinationUrl, out work))
					break;

                if (synchronizationQueue.IsDifferentWorkForTheSameFileBeingPerformed(work, destinationUrl))
				{
					Log.Debug("There was an already being performed synchronization of a file '{0}' to {1}", work.FileName,
							  destination);

                    if (synchronizationQueue.EnqueueSynchronization(destinationUrl, work)) // add it again at the end of the queue
                    {
                        // add it again at the end of the queue
                        publisher.Publish(new SynchronizationUpdateNotification
                        {
                            FileSystemName = systemConfiguration.FileSystemName,
                            FileName = work.FileName,
                            DestinationFileSystemUrl = destinationUrl,
                            SourceServerId = storage.Id,
                            SourceFileSystemUrl = FileSystemUrl,
                            Type = work.SynchronizationType,
                            Action = SynchronizationAction.Enqueue,
                            SynchronizationDirection = SynchronizationDirection.Outgoing
                        });
                    }
				}
				else
				{
					var workTask = PerformSynchronizationAsync(destination, work);

					if (forceSyncingContinuation)
					{
						workTask.ContinueWith(t => SynchronizePendingFilesAsync(destination, true).ToArray());
					}
					yield return workTask;
				}
			}
		}

        private async Task<SynchronizationReport> PerformSynchronizationAsync(IAsyncFilesSynchronizationCommands destination,
																			  SynchronizationWorkItem work)
		{
            var commands = (IAsyncFilesCommandsImpl)destination.Commands;
            string destinationUrl = commands.UrlFor();

			Log.Debug("Starting to perform {0} for a file '{1}' and a destination server {2}",
                       work.GetType().Name, work.FileName, destinationUrl);

            if (!CanSynchronizeTo(destinationUrl))
			{
				Log.Debug("The limit of active synchronizations to {0} server has been achieved. Cannot process a file '{1}'.",
                          destinationUrl, work.FileName);

                if (synchronizationQueue.EnqueueSynchronization(destinationUrl, work))
                {
                    publisher.Publish(new SynchronizationUpdateNotification
                    {
                        FileSystemName = systemConfiguration.FileSystemName,
                        FileName = work.FileName,
                        DestinationFileSystemUrl = destinationUrl,
                        SourceServerId = storage.Id,
                        SourceFileSystemUrl = FileSystemUrl,
                        Type = work.SynchronizationType,
                        Action = SynchronizationAction.Enqueue,
                        SynchronizationDirection = SynchronizationDirection.Outgoing
                    });
                }

				return new SynchronizationReport(work.FileName, work.FileETag, work.SynchronizationType)
				{
					Exception = new SynchronizationException(string.Format(
						"The limit of active synchronizations to {0} server has been achieved. Cannot process a file '{1}'.",
                        destinationUrl, work.FileName))
				};
			}

			string fileName = work.FileName;
            synchronizationQueue.SynchronizationStarted(work, destinationUrl);
			publisher.Publish(new SynchronizationUpdateNotification
			{
                FileSystemName = systemConfiguration.FileSystemName,
				FileName = work.FileName,
                DestinationFileSystemUrl = destinationUrl,
				SourceServerId = storage.Id,
				SourceFileSystemUrl = FileSystemUrl,
				Type = work.SynchronizationType,
				Action = SynchronizationAction.Start,
				SynchronizationDirection = SynchronizationDirection.Outgoing
			});

			SynchronizationReport report;

			try
			{
				report = await work.PerformAsync(destination);
			}
			catch (Exception ex)
			{
				report = new SynchronizationReport(work.FileName, work.FileETag, work.SynchronizationType)
				{
					Exception = ex,
				};
			}

			var synchronizationCancelled = false;

			if (report.Exception == null)
			{
				var moreDetails = string.Empty;

				if (work.SynchronizationType == SynchronizationType.ContentUpdate)
				{
					moreDetails = string.Format(". {0} bytes were transfered and {1} bytes copied. Need list length was {2}",
												report.BytesTransfered, report.BytesCopied, report.NeedListLength);
				}

                UpdateSuccessfulSynchronizationTime();

                Log.Debug("{0} to {1} has finished successfully{2}", work.ToString(), destinationUrl, moreDetails);
			}
			else
			{
				if (work.IsCancelled || report.Exception is TaskCanceledException)
				{
					synchronizationCancelled = true;
                    Log.DebugException(string.Format("{0} to {1} was cancelled", work, destinationUrl), report.Exception);
				}
				else
				{
                    Log.WarnException(string.Format("{0} to {1} has finished with the exception", work, destinationUrl),
									  report.Exception);
				}
			}

            Queue.SynchronizationFinished(work, destinationUrl);

			if (!synchronizationCancelled)
                CreateSyncingConfiguration(fileName, work.FileETag, destinationUrl, work.SynchronizationType);

			publisher.Publish(new SynchronizationUpdateNotification
			{
                FileSystemName = systemConfiguration.FileSystemName,
				FileName = work.FileName,
                DestinationFileSystemUrl = destinationUrl,
				SourceServerId = storage.Id,
				SourceFileSystemUrl = FileSystemUrl,
				Type = work.SynchronizationType,
				Action = SynchronizationAction.Finish,
				SynchronizationDirection = SynchronizationDirection.Outgoing
			});

			return report;
		}

		private IEnumerable<FileHeaderInformation> GetFilesToSynchronization( SourceSynchronizationInformation destinationsSynchronizationInformationForSource, int take)
		{
			var filesToSynchronization = new List<FileHeaderInformation>();

			Log.Debug("Getting files to synchronize with ETag greater than {0} [parameter take = {1}]",
					  destinationsSynchronizationInformationForSource.LastSourceFileEtag, take);

			try
			{
				storage.Batch(
					accessor =>
					filesToSynchronization =
					accessor.GetFilesAfter(destinationsSynchronizationInformationForSource.LastSourceFileEtag, take).ToList());
			}
			catch (Exception e)
			{
				Log.WarnException(
					string.Format("Could not get files to synchronize after: " +
								  destinationsSynchronizationInformationForSource.LastSourceFileEtag), e);
			}

			return filesToSynchronization;
		}

		private Task<SynchronizationConfirmation[]> ConfirmPushedFiles(IList<SynchronizationDetails> filesNeedConfirmation, IAsyncFilesSynchronizationCommands destinationClient)
		{
			if (filesNeedConfirmation.Count == 0)
			{
				return new CompletedTask<SynchronizationConfirmation[]>(new SynchronizationConfirmation[0]);
			}
			return destinationClient.GetConfirmationForFilesAsync(filesNeedConfirmation.Select(x => new Tuple<string, Guid>(x.FileName, x.FileETag)));
		}

        private IEnumerable<SynchronizationDetails> GetSyncingConfigurations(SynchronizationDestination destination)
		{
			IList<SynchronizationDetails> configObjects = new List<SynchronizationDetails>();

			try
			{
				storage.Batch(
					accessor =>
					{
						configObjects = accessor.GetConfigsStartWithPrefix(RavenFileNameHelper.SyncNamePrefix + Uri.EscapeUriString(destination.FileSystemUrl), 0, 100)
									            .Select(config => config.JsonDeserialization<SynchronizationDetails>())
                                                .ToList();
					});
			}
			catch (Exception e)
			{
				Log.WarnException(string.Format("Could not get syncing configurations for a destination {0}", destination), e);
			}

			return configObjects;
		}

        private void CreateSyncingConfiguration(string fileName, Guid etag, string destinationFileSystemUrl, SynchronizationType synchronizationType)
		{
			try
			{
				var name = RavenFileNameHelper.SyncNameForFile(fileName, destinationFileSystemUrl);

                var details = new SynchronizationDetails
				{
					DestinationUrl = destinationFileSystemUrl,
					FileName = fileName,
					FileETag = etag,
					Type = synchronizationType
				};

				storage.Batch(accessor => accessor.SetConfig(name, JsonExtensions.ToJObject(details)));
			}
			catch (Exception e)
			{
				Log.WarnException(
					string.Format("Could not create syncing configurations for a file {0} and destination {1}", fileName, destinationFileSystemUrl),
					e);
			}
		}

		private void RemoveSyncingConfiguration(string fileName, string destination)
		{
			try
			{
				var name = RavenFileNameHelper.SyncNameForFile(fileName, destination);
				storage.Batch(accessor => accessor.DeleteConfig(name));
			}
			catch (Exception e)
			{
				Log.WarnException(
					string.Format("Could not remove syncing configurations for a file {0} and a destination {1}", fileName, destination),
					e);
			}
		}

        private RavenJObject GetLocalMetadata(string fileName)
		{
            RavenJObject result = null;
            try
            {
                storage.Batch(accessor => { result = accessor.GetFile(fileName, 0, 0).Metadata; });
            }
            catch (FileNotFoundException)
            {
                return null;
            }
            FileAndPagesInformation fileAndPages = null;
            {
                try
                {
                    storage.Batch(accessor => fileAndPages = accessor.GetFile(fileName, 0, 0));
                }
                catch (FileNotFoundException)
                {

                }
            }

            return result;
		}

		private IEnumerable<SynchronizationDestination> GetSynchronizationDestinations()
		{			
            var destinationsConfigExists = false;
			storage.Batch(accessor => destinationsConfigExists = accessor.ConfigExists(SynchronizationConstants.RavenSynchronizationDestinations));
         
			if (!destinationsConfigExists)
			{
				if (failedAttemptsToGetDestinationsConfig < 3 || failedAttemptsToGetDestinationsConfig % 10 == 0)
				{
					Log.Debug("Configuration " + SynchronizationConstants.RavenSynchronizationDestinations + " does not exist");
				}

				failedAttemptsToGetDestinationsConfig++;

                yield break;
			}

			failedAttemptsToGetDestinationsConfig = 0;

			var destinationsConfig = new RavenJObject();

			storage.Batch(accessor => destinationsConfig = accessor.GetConfig(SynchronizationConstants.RavenSynchronizationDestinations));

            var destinationsStrings = destinationsConfig.Value<RavenJArray>("Destinations");
            if (destinationsStrings == null)
            {
                Log.Warn("Empty " + SynchronizationConstants.RavenSynchronizationDestinations + " configuration");
                yield break;
            }
            if (destinationsStrings.Count() == 0)
            {
                Log.Warn("Configuration " + SynchronizationConstants.RavenSynchronizationDestinations + " does not contain any destination");
                yield break;
            }
            
            foreach ( var token in destinationsStrings )
            {
                yield return JsonExtensions.JsonDeserialization<SynchronizationDestination>((RavenJObject)token);
            }
		}

        private bool CanSynchronizeTo(string destinationFileSystemUrl)
		{
			return LimitOfConcurrentSynchronizations() > synchronizationQueue.NumberOfActiveSynchronizationTasksFor(destinationFileSystemUrl);
		}

        private int AvailableSynchronizationRequestsTo(string destinationFileSystemUrl)
		{
			return LimitOfConcurrentSynchronizations() - synchronizationQueue.NumberOfActiveSynchronizationTasksFor(destinationFileSystemUrl);
		}

		private int LimitOfConcurrentSynchronizations()
		{
			bool limit = false;
			int configuredLimit = 0;

			storage.Batch(
				accessor =>
				limit = accessor.TryGetConfigurationValue(SynchronizationConstants.RavenSynchronizationLimit, out configuredLimit));

			return limit ? configuredLimit : DefaultLimitOfConcurrentSynchronizations;
		}

		public void Cancel(string fileName)
		{
			Log.Debug("Cancellation of active synchronizations of a file '{0}'", fileName);
			Queue.CancelActiveSynchronizations(fileName);
		}

        private void UpdateSuccessfulSynchronizationTime()
        {
            LastSuccessfulSynchronizationTime = SystemTime.UtcNow;
        }

		private static void LogFilesInfo(string message, ICollection<FileHeaderInformation> files)
		{
			Log.Debug(message, files.Count,
					  string.Join(",", files.Select(x => string.Format("{0} [ETag {1}]", x.Name, x.Metadata.Value<Guid>("ETag")))));
		}
	}
}