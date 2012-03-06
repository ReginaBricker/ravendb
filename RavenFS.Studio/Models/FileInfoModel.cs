﻿using System.Windows.Input;
using RavenFS.Client;
using RavenFS.Studio.Commands;
using RavenFS.Studio.Infrastructure;

namespace RavenFS.Studio.Models
{
	public class FileInfoModel : Model
	{
		public string Name { get; set; }

		public FileInfoModel()
		{
			Name = new UrlParser(UrlUtil.Url).GetQueryParam("name");

            ApplicationModel.Current.Client.GetMetadataForAsync(Name)
				.ContinueWith(task => Metadata = task.Result);
		}

		private NameValueCollection metadata;

		public NameValueCollection Metadata
		{
			get { return metadata; }
			set { metadata = value; OnPropertyChanged();}
		}

	}
}