﻿using Raven.Client.Connection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Connection
{
    public interface IAsyncFilesCommandsImpl : IAsyncFilesCommands
    {   
        string ServerUrl { get; }

        HttpJsonRequestFactory RequestFactory { get; }

        IFilesReplicationInformer ReplicationInformer { get; }
    }
}
