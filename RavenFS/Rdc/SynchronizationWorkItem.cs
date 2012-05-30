﻿namespace RavenFS.Rdc
{
	using System.Threading.Tasks;
	using Client;

	public abstract class SynchronizationWorkItem
	{
		protected SynchronizationWorkItem(string fileName, string sourceServerUrl)
		{
			FileName = fileName;
			SourceServerUrl = sourceServerUrl;
		}

		public string FileName { get; private set; }
		public string SourceServerUrl { get; private set; }

		public abstract Task<SynchronizationReport> Perform(string destination);
	}
}