//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Configuration.Install;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Security.Principal;
using System.ServiceProcess;
using System.Text;
using System.Windows.Forms;
using System.Xml;
using Microsoft.Win32;
using NDesk.Options;
using NLog.Config;
using Raven.Abstractions;
using Raven.Abstractions.Logging;
using Raven.Database;
using Raven.Database.Actions;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Util;

namespace Raven.Server
{
	using Raven.Abstractions.Util;

	public static class Program
	{
		static string[] cmdLineArgs;
		private static void Main(string[] args)
		{
			cmdLineArgs = args;
			if (RunningInInteractiveMode(args))
			{
				try
				{
					LogManager.EnsureValidLogger();
					InteractiveRun(args);
				}
				catch (ReflectionTypeLoadException e)
				{
					WaitForUserInputAndExitWithError(GetLoaderExceptions(e), args);
				}
				catch (InvalidOperationException e)
				{
					ReflectionTypeLoadException refEx = null;
					if (e.InnerException != null)
					{
						refEx = e.InnerException.InnerException as ReflectionTypeLoadException;
					}
					var errorMessage = refEx != null ? GetLoaderExceptions(refEx) : e.ToString();

					WaitForUserInputAndExitWithError(errorMessage, args);
				}
				catch (Exception e)
				{

					EmitWarningInRed();

					WaitForUserInputAndExitWithError(e.ToString(), args);
				}
			}
			else
			{
				// no try catch here, we want the exception to be logged by Windows
				ServiceBase.Run(new RavenService());
			}
		}

		private static string GetLoaderExceptions(ReflectionTypeLoadException exception)
		{
			var sb = new StringBuilder();
			sb.AppendLine(exception.ToString());
			foreach (var loaderException in exception.LoaderExceptions)
			{
				sb.AppendLine("- - - -").AppendLine();
				sb.AppendLine(loaderException.ToString());
			}

			return sb.ToString();
		}

		private static bool RunningInInteractiveMode(string[] args)
		{
			if (Type.GetType("Mono.Runtime") != null) // running on mono, which doesn't support detecting this
				return true;
			return Environment.UserInteractive || (args != null && args.Length > 0);
		}

		private static void WaitForUserInputAndExitWithError(string msg, string[] args)
		{
			EmitWarningInRed();

			Console.Error.WriteLine(msg);

			if (args.Contains("--msgbox", StringComparer.OrdinalIgnoreCase) ||
				args.Contains("/msgbox", StringComparer.OrdinalIgnoreCase))
			{
				MessageBox.Show(msg, "RavenDB Startup failure");
			}
			Console.WriteLine("Press any key to continue...");
			try
			{
				Console.ReadKey(true);
			}
			catch
			{
				// cannot read key?
			}
			Environment.Exit(-1);
		}

		private static void EmitWarningInRed()
		{
			var old = Console.ForegroundColor;
			Console.ForegroundColor = ConsoleColor.Red;
			Console.Error.WriteLine("A critical error occurred while starting the server. Please see the exception details bellow for more details:");
			Console.ForegroundColor = old;
		}

		private static void InteractiveRun(string[] args)
		{
			string backupLocation = null;
			string restoreLocation = null;
			bool defrag = false;
			string theUser = null;
			Action actionToTake = null;
			bool launchBrowser = false;
			bool noLog = false;
			var ravenConfiguration = new RavenConfiguration();

			OptionSet optionSet = null;
			optionSet = new OptionSet
			{
				{"set={==}", "The configuration {0:option} to set to the specified {1:value}" , (key, value) =>
				{
					ravenConfiguration.Settings[key] = value;
					ravenConfiguration.Initialize();
				}},
				{"nolog", "Don't use the default log", s => noLog=true},
				{"config=", "The config {0:file} to use", ravenConfiguration.LoadFrom},
				{"install", "Installs the RavenDB service", key => actionToTake= () => AdminRequired(InstallAndStart)},
				{"user=", "Which user will be used", user=> theUser = user},
				{"setup-perf-counters", "Setup the performance counters and the related permissions", key => actionToTake = ()=> AdminRequired(()=>SetupPerfCounters(theUser))},
				{"allow-blank-password-use", "Allow to log on by using a Windows account that has a blank password", key => actionToTake = () => AdminRequired(() => SetLimitBlankPasswordUseRegValue(0))},
				{"deny-blank-password-use", "Deny to log on by using a Windows account that has a blank password", key => actionToTake = () =>  AdminRequired(() => SetLimitBlankPasswordUseRegValue(1))},
				{"service-name=", "The {0:service name} to use when installing or uninstalling the service, default to RavenDB", name => ProjectInstaller.SERVICE_NAME = name},
				{"uninstall", "Uninstalls the RavenDB service", key => actionToTake= () => AdminRequired(EnsureStoppedAndUninstall)},
				{"start", "Starts the RavenDB service", key => actionToTake= () => AdminRequired(StartService)},
				{"restart", "Restarts the RavenDB service", key => actionToTake= () => AdminRequired(RestartService)},
				{"stop", "Stops the RavenDB service", key => actionToTake= () => AdminRequired(StopService)},
				{"ram", "Run RavenDB in RAM only", key =>
				{
					ravenConfiguration.Settings["Raven/RunInMemory"] = "true";
					ravenConfiguration.RunInMemory = true;
					actionToTake = () => RunInDebugMode(AnonymousUserAccessMode.Admin, ravenConfiguration, launchBrowser, noLog);		
				}},
				{"debug", "Runs RavenDB in debug mode", key => actionToTake = () => RunInDebugMode(null, ravenConfiguration, launchBrowser, noLog)},
				{"browser|launchbrowser", "After the server starts, launches the browser", key => launchBrowser = true},
				{"help", "Help about the command line interface", key =>
				{
					actionToTake = () => PrintUsage(optionSet);
				}},
				{"config-help", "Help about configuration options", key=>
				{
					actionToTake = () => PrintConfig(ravenConfiguration.GetConfigOptionsDocs());
				}},
				{"restore", 
					"Restores a RavenDB database from backup",
					key => actionToTake = () =>
					{
						if(backupLocation == null || restoreLocation == null)
						{
							throw new OptionException("when using restore, source and destination must be specified", "restore");
						}
						RunRestoreOperation(backupLocation, restoreLocation, defrag);
					}},
				{"defrag", 
					"Applicable only during restore, execute defrag after the restore is completed", key =>
					{
						defrag = true;
					}},
				{"dest=|destination=", "The {0:path} of the new new database", value => restoreLocation = value},
				{"src=|source=", "The {0:path} of the backup", value => backupLocation = value},
				{"encrypt-self-config", "Encrypt the RavenDB configuration file", file =>
						{
							actionToTake = () => ProtectConfiguration(AppDomain.CurrentDomain.SetupInformation.ConfigurationFile);
						}},
				{"encrypt-config=", "Encrypt the specified {0:configuration file}", file =>
						{
							actionToTake = () => ProtectConfiguration(file);
						}},
				{"decrypt-self-config", "Decrypt the RavenDB configuration file", file =>
						{
							actionToTake = () => UnprotectConfiguration(AppDomain.CurrentDomain.SetupInformation.ConfigurationFile);
						}},
				{"decrypt-config=", "Decrypt the specified {0:configuration file}", file =>
						{
							actionToTake = () => UnprotectConfiguration(file);
						}},
				{"installSSL={==}", "Bind X509 certificate specified in {0:option} with optional password from {1:option} with 'Raven/Port'.", (sslCertificateFile, sslCertificatePassword) =>
						{
							actionToTake = () => InstallSsl(sslCertificateFile, sslCertificatePassword, ravenConfiguration);
						}},
				{"uninstallSSL={==}", "Unbind X509 certificate specified in {0:option} with optional password from {2:option} from 'Raven/Port'.", (sslCertificateFile, sslCertificatePassword) =>
						{
							actionToTake = () => UninstallSsl(sslCertificateFile, sslCertificatePassword, ravenConfiguration);
						}}
			};


			try
			{
				if (args.Length == 0) // we default to executing in debug mode 
					args = new[] { "--debug" };

				optionSet.Parse(args);
			}
			catch (Exception e)
			{
				Console.WriteLine(e.Message);
				PrintUsage(optionSet);
				return;
			}

			if (actionToTake == null)
				actionToTake = () => RunInDebugMode(null, ravenConfiguration, launchBrowser, noLog);

			actionToTake();

		}

		public static void DumpToCsv(RavenConfiguration ravenConfiguration)
		{
			using (var db = new DocumentDatabase(ravenConfiguration))
			{
				db.TransactionalStorage.DumpAllStorageTables();
			}
		}

		private static void InstallSsl(string sslCertificateFile, string sslCertificatePassword, RavenConfiguration configuration)
		{
			if (string.IsNullOrEmpty(sslCertificateFile))
				throw new InvalidOperationException("X509 certificate path cannot be empty.");

			var certificate = !string.IsNullOrEmpty(sslCertificatePassword) ? new X509Certificate2(sslCertificateFile, sslCertificatePassword) : new X509Certificate2(sslCertificateFile);

			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(configuration.Port, true);
			NonAdminHttp.UnbindCertificate(configuration.Port, certificate);
			NonAdminHttp.BindCertificate(configuration.Port, certificate);
		}

		private static void UninstallSsl(string sslCertificateFile, string sslCertificatePassword, RavenConfiguration configuration)
		{
			X509Certificate2 certificate = null;

			if (!string.IsNullOrEmpty(sslCertificateFile))
			{
				certificate = !string.IsNullOrEmpty(sslCertificatePassword) ? new X509Certificate2(sslCertificateFile, sslCertificatePassword) : new X509Certificate2(sslCertificateFile);
			}

			NonAdminHttp.UnbindCertificate(configuration.Port, certificate);
		}

		private static void SetupPerfCounters(string user)
		{
			user = user ?? WindowsIdentity.GetCurrent().Name;
			PerformanceCountersUtils.EnsurePerformanceCountersMonitoringAccess(user);

			var actionToTake = user.StartsWith("IIS") ? "restart IIS service" : "log in the user again";

			Console.Write("User {0} has been added to Performance Monitoring Users group. Please {1} to take an effect.", user, actionToTake);
		}

		private static void SetLimitBlankPasswordUseRegValue(int value)
		{
			// value == 0 - disable a limit
			// value == 1 - enable a limit

			if (value != 0 && value != 1)
				throw new ArgumentException("Allowed arguments for 'LimitBlankPasswordUse' registry value are only 0 or 1", "value");

			const string registryKey = @"SYSTEM\CurrentControlSet\Control\Lsa";
			const string policyName = "Limit local account use of blank passwords to console logon only";

			var lsaKey = Registry.LocalMachine.OpenSubKey(registryKey, true);
			if (lsaKey != null)
			{
				lsaKey.SetValue("LimitBlankPasswordUse", value, RegistryValueKind.DWord);

				if (value == 0)
					Console.WriteLine("You have just disabled the following security policy: '{0}' on the local machine.", policyName);
				else
					Console.WriteLine("You have just enabled the following security policy: '{0}' on the local machine.", policyName);
			}
			else
			{
				Console.WriteLine("Error: Could not find the registry key '{0}' in order to disable '{1}' policy.", registryKey,
								  policyName);
			}
		}

		private static void ProtectConfiguration(string file)
		{
			if (string.Equals(Path.GetExtension(file), ".config", StringComparison.OrdinalIgnoreCase))
				file = Path.GetFileNameWithoutExtension(file);

			var configuration = ConfigurationManager.OpenExeConfiguration(file);
			var names = new[] { "appSettings", "connectionStrings" };

			foreach (var section in names.Select(configuration.GetSection))
			{
				section.SectionInformation.ProtectSection("RsaProtectedConfigurationProvider");
				section.SectionInformation.ForceSave = true;
			}

			configuration.Save(ConfigurationSaveMode.Full);
		}

		private static void UnprotectConfiguration(string file)
		{
			if (string.Equals(Path.GetExtension(file), ".config", StringComparison.OrdinalIgnoreCase))
				file = Path.GetFileNameWithoutExtension(file);

			var configuration = ConfigurationManager.OpenExeConfiguration(file);
			var names = new[] { "appSettings", "connectionStrings" };

			foreach (var section in names.Select(configuration.GetSection))
			{
				section.SectionInformation.UnprotectSection();
				section.SectionInformation.ForceSave = true;
			}
			configuration.Save(ConfigurationSaveMode.Full);
		}

		private static void PrintConfig(IEnumerable<string> configOptions)
		{
			Console.WriteLine(
				@"
Raven DB
Document Database for the .Net Platform
----------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------
Configuration options:
",
				SystemTime.UtcNow.Year);

			foreach (var configOptionDoc in configOptions)
			{
				Console.WriteLine(configOptionDoc);
				Console.WriteLine();
			}
		}

		private static void RunRestoreOperation(string backupLocation, string databaseLocation, bool defrag)
		{
			try
			{
				var ravenConfiguration = new RavenConfiguration();
				if (File.Exists(Path.Combine(backupLocation, "Raven.voron")))
                {
                    ravenConfiguration.DefaultStorageTypeName = typeof(Raven.Storage.Voron.TransactionalStorage).AssemblyQualifiedName;                    
                }
				else if (Directory.Exists(Path.Combine(backupLocation, "new")))
				{
					ravenConfiguration.DefaultStorageTypeName = typeof(Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName;
				}
				DocumentDatabase.Restore(ravenConfiguration, backupLocation, databaseLocation, Console.WriteLine, defrag);
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
			}
		}

		private static void AdminRequired(Action actionThatMayRequiresAdminPrivileges)
		{
			var principal = new WindowsPrincipal(WindowsIdentity.GetCurrent());
			if (principal.IsInRole(WindowsBuiltInRole.Administrator) == false)
			{
				if (RunAgainAsAdmin())
					return;
			}
			actionThatMayRequiresAdminPrivileges();
		}

		private static bool RunAgainAsAdmin()
		{
			try
			{
				for (var i = 0; i < cmdLineArgs.Length; i++)
				{
					if (cmdLineArgs[i].Contains(" "))
					{
						cmdLineArgs[i] = "\"" + cmdLineArgs[i] + "\"";
					}
				}

				var process = Process.Start(new ProcessStartInfo
				{
					Arguments = string.Join(" ", cmdLineArgs),
					FileName = Assembly.GetExecutingAssembly().Location,
					Verb = "runas",
				});
				process.WaitForExit();
				return true;
			}
			catch (Exception)
			{
				return false;
			}
		}

		private static void RunInDebugMode(
			AnonymousUserAccessMode? anonymousUserAccessMode,
			RavenConfiguration ravenConfiguration,
			bool launchBrowser,
			bool noLog)
		{
			if (noLog == false)
				ConfigureDebugLogging();

			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(ravenConfiguration.Port, ravenConfiguration.UseSsl);
			if (anonymousUserAccessMode.HasValue)
				ravenConfiguration.AnonymousUserAccessMode = anonymousUserAccessMode.Value;
			while (RunServerInDebugMode(ravenConfiguration, launchBrowser))
			{
				launchBrowser = false;
			}
		}

		private static void ConfigureDebugLogging()
		{
			var nlogPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "NLog.config");
			if (File.Exists(nlogPath))
				return;// that overrides the default config

			using (var stream = typeof(Program).Assembly.GetManifestResourceStream("Raven.Server.DefaultLogging.config"))
			using (var reader = XmlReader.Create(stream))
			{
				NLog.LogManager.Configuration = new XmlLoggingConfiguration(reader, "default-config");
			}
		}

		private static bool RunServerInDebugMode(RavenConfiguration ravenConfiguration, bool launchBrowser)
		{
			var sp = Stopwatch.StartNew();
			using (var server = new RavenDbServer(ravenConfiguration))
			{
				sp.Stop();
				var path = Path.Combine(Environment.CurrentDirectory, "default.raven");
				if (File.Exists(path))
				{
					Console.WriteLine("Loading data from: {0}", path);
					//new SmugglerApi(new SmugglerOptions(), new RavenConnectionStringOptions {Url = ravenConfiguration.ServerUrl}).ImportData(new SmugglerOptions {BackupPath = path});
				}

				Console.WriteLine("Raven is ready to process requests. Build {0}, Version {1}", DocumentDatabase.BuildVersion, DocumentDatabase.ProductVersion);
				Console.WriteLine("Server started in {0:#,#;;0} ms", sp.ElapsedMilliseconds);
				Console.WriteLine("Data directory: {0}", ravenConfiguration.RunInMemory ? "RAM" : ravenConfiguration.DataDirectory);
				Console.WriteLine("HostName: {0} Port: {1}, Storage: {2}", ravenConfiguration.HostName ?? "<any>",
					ravenConfiguration.Port,
					server.SystemDatabase.TransactionalStorage.FriendlyName);
				Console.WriteLine("Server Url: {0}", ravenConfiguration.ServerUrl);

				if (launchBrowser)
				{
					try
					{
						Process.Start(ravenConfiguration.ServerUrl);
					}
					catch (Exception e)
					{
						Console.WriteLine("Could not start browser: " + e.Message);
					}
				}
				return InteractiveRun(server);
			}
		}

		private static bool InteractiveRun(RavenDbServer server)
		{
			bool? done = null;
			var actions = new Dictionary<string, Action>
			              {
				              { "cls", TryClearingConsole },
				              {
					              "reset", () =>
					              {
						              TryClearingConsole();
						              done = true;
					              }
				              },
				              {
					              "gc", () =>
					              {
						              long before = Process.GetCurrentProcess().WorkingSet64;
						              Console.WriteLine(
										  "Starting garbage collection (without LOH compaction), current memory is: {0:#,#.##;;0} MB",
							              before / 1024d / 1024d);
						              RavenGC.CollectGarbage(false, () => server.SystemDatabase.TransactionalStorage.ClearCaches());
						              var after = Process.GetCurrentProcess().WorkingSet64;
						              Console.WriteLine(
							              "Done garbage collection, current memory is: {0:#,#.##;;0} MB, saved: {1:#,#.##;;0} MB",
							              after / 1024d / 1024d,
							              (before - after) / 1024d / 1024d);
					              }
				              },
				              {
					              "loh-compaction", () =>
					              {
						              long before = Process.GetCurrentProcess().WorkingSet64;
						              Console.WriteLine(
							              "Starting garbage collection (with LOH compaction), current memory is: {0:#,#.##;;0} MB",
							              before / 1024d / 1024d);
									  RavenGC.CollectGarbage(true, () => server.SystemDatabase.TransactionalStorage.ClearCaches());
						              var after = Process.GetCurrentProcess().WorkingSet64;
						              Console.WriteLine(
							              "Done garbage collection, current memory is: {0:#,#.##;;0} MB, saved: {1:#,#.##;;0} MB",
							              after / 1024d / 1024d,
							              (before - after) / 1024d / 1024d);
					              }
				              },
				              { "q", () => done = false }
			              };

			WriteInteractiveOptions(actions);
			while (true)
			{
				var readLine = Console.ReadLine() ?? "";

				Action value;
				if (actions.TryGetValue(readLine, out value) == false)
				{
					Console.WriteLine("Could not understand: {0}", readLine);
					WriteInteractiveOptions(actions);
					continue;
				}

				value();
				if (done != null)
					return done.Value;
			}
		}

		private static void TryClearingConsole()
		{
			try
			{
				Console.Clear();
			}
			catch (IOException)
			{
				// redirected output, probably, ignoring
			}
		}

		private static void WriteInteractiveOptions(Dictionary<string, Action> actions)
		{
			Console.WriteLine("Available commands: {0}", string.Join(", ", actions.Select(x => x.Key)));
		}

		private static void PrintUsage(OptionSet optionSet)
		{
			Console.WriteLine(
				@"
Raven DB
Document Database for the .Net Platform
----------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------
Command line options:",
				SystemTime.UtcNow.Year);

			optionSet.WriteOptionDescriptions(Console.Out);

			Console.WriteLine(@"
Enjoy...
");
		}

		private static void EnsureStoppedAndUninstall()
		{
			if (ServiceIsInstalled() == false)
			{
				Console.WriteLine("Service is not installed");
			}
			else
			{
				var stopController = new ServiceController(ProjectInstaller.SERVICE_NAME);

				if (stopController.Status == ServiceControllerStatus.Running)
					stopController.Stop();

				ManagedInstallerClass.InstallHelper(new[] { "/u", Assembly.GetExecutingAssembly().Location });
			}
		}

		private static void StopService()
		{
			var stopController = new ServiceController(ProjectInstaller.SERVICE_NAME);

			if (stopController.Status == ServiceControllerStatus.Running)
			{
				stopController.Stop();
				stopController.WaitForStatus(ServiceControllerStatus.Stopped);
			}
		}


		private static void StartService()
		{
			var stopController = new ServiceController(ProjectInstaller.SERVICE_NAME);

			if (stopController.Status != ServiceControllerStatus.Running)
			{
				stopController.Start();
				stopController.WaitForStatus(ServiceControllerStatus.Running);
			}
		}

		private static void RestartService()
		{
			var stopController = new ServiceController(ProjectInstaller.SERVICE_NAME);

			if (stopController.Status == ServiceControllerStatus.Running)
			{
				stopController.Stop();
				stopController.WaitForStatus(ServiceControllerStatus.Stopped);
			}
			if (stopController.Status != ServiceControllerStatus.Running)
			{
				stopController.Start();
				stopController.WaitForStatus(ServiceControllerStatus.Running);
			}

		}

		private static void InstallAndStart()
		{
			if (ServiceIsInstalled())
			{
				Console.WriteLine("Service is already installed");
			}
			else
			{
				ManagedInstallerClass.InstallHelper(new[] { Assembly.GetExecutingAssembly().Location });
				SetRecoveryOptions(ProjectInstaller.SERVICE_NAME);
				var startController = new ServiceController(ProjectInstaller.SERVICE_NAME);
				startController.Start();
			}
		}

		private static bool ServiceIsInstalled()
		{
			return (ServiceController.GetServices().Count(s => s.ServiceName == ProjectInstaller.SERVICE_NAME) > 0);
		}

		static void SetRecoveryOptions(string serviceName)
		{
			int exitCode;
			var arguments = string.Format("failure \"{0}\" reset= 500 actions= restart/60000", serviceName);
			using (var process = new Process())
			{
				var startInfo = process.StartInfo;
				startInfo.FileName = "sc";
				startInfo.WindowStyle = ProcessWindowStyle.Hidden;

				// tell Windows that the service should restart if it fails
				startInfo.Arguments = arguments;

				process.Start();
				process.WaitForExit();

				exitCode = process.ExitCode;

				process.Close();
			}

			if (exitCode != 0)
				throw new InvalidOperationException(
					"Failed to set the service recovery policy. Command: " + Environment.NewLine + "sc " + arguments + Environment.NewLine + "Exit code: " + exitCode);
		}
	}
}
