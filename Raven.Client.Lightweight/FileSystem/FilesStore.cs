﻿using Raven.Abstractions.Connection;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.FileSystem.Changes;
using Raven.Client.FileSystem.Connection;
using Raven.Client.FileSystem.Extensions;
using Raven.Client.Util;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;


namespace Raven.Client.FileSystem
{
    public class FilesStore : IFilesStore
    {
        /// <summary>
        /// The current session id - only used during construction
        /// </summary>
        [ThreadStatic]
        protected static Guid? currentSessionId;

        private HttpJsonRequestFactory jsonRequestFactory;
        private FilesConvention conventions;
        private readonly AtomicDictionary<IFilesChanges> fileSystemChanges = new AtomicDictionary<IFilesChanges>(StringComparer.OrdinalIgnoreCase);
        private readonly AtomicDictionary<IAsyncFilesCommandsImpl> fileSystemCommands = new AtomicDictionary<IAsyncFilesCommandsImpl>(StringComparer.OrdinalIgnoreCase);
        

        private bool initialized;
        private FilesSessionListeners listeners = new FilesSessionListeners();

        private const int DefaultNumberOfCachedRequests = 2048;
        private int maxNumberOfCachedRequests = DefaultNumberOfCachedRequests;

        public FilesStore()
        {
            Credentials = CredentialCache.DefaultNetworkCredentials;

            SharedOperationsHeaders = new NameValueCollection();
            Conventions = new FilesConvention();
        }

        /// <summary>
        /// Gets or sets the credentials.
        /// </summary>
        /// <value>The credentials.</value>
        public ICredentials Credentials 
        {
            get { return this.credentials; }
            set { this.credentials = credentials ?? CredentialCache.DefaultNetworkCredentials; }
        }
        private ICredentials credentials;

        /// <summary>
        /// The API Key to use when authenticating against a RavenDB server that
        /// supports API Key authentication
        /// </summary>
        public string ApiKey { get; set; }

        public IFilesChanges Changes(string filesystem = null)
        {
            AssertInitialized();

            if (string.IsNullOrWhiteSpace(filesystem))
                filesystem = this.DefaultFileSystem;

            return fileSystemChanges.GetOrAdd(filesystem, CreateFileSystemChanges);
        }

        protected virtual IFilesChanges CreateFileSystemChanges(string filesystem)
        {
            if (string.IsNullOrEmpty(Url))
                throw new InvalidOperationException("Changes API requires usage of server/client");

            var tenantUrl = Url + "/fs/" + filesystem;

            var commands = fileSystemCommands.GetOrAdd(filesystem, x => (IAsyncFilesCommandsImpl)this.AsyncFilesCommands.ForFileSystem(x));

            using (NoSynchronizationContext.Scope())
            {
                return new FilesChangesClient(tenantUrl,
                    ApiKey,
                    Credentials,
                    jsonRequestFactory,
                    Conventions,
                    commands.ReplicationInformer,
                    () => {
                        fileSystemChanges.Remove(filesystem);
                        fileSystemCommands.Remove(filesystem);
                    });
            }
        }

        /// <summary>
        /// Gets the shared operations headers.
        /// </summary>
        /// <value>The shared operations headers.</value>
        public virtual NameValueCollection SharedOperationsHeaders { get; protected set; }

        /// <summary>
        /// Whatever this instance has json request factory available
        /// </summary>
        public virtual bool HasJsonRequestFactory
        {
            get { return true; }
        }

        ///<summary>
        /// Get the <see cref="HttpJsonRequestFactory"/> for the stores
        ///</summary>
        public virtual HttpJsonRequestFactory JsonRequestFactory
        {
            get
            {
                return jsonRequestFactory;
            }
        }

        public string DefaultFileSystem
        {
            get; set;
        }

        /// <summary>
        /// Gets the conventions.
        /// </summary>
        /// <value>The conventions.</value>
        public virtual FilesConvention Conventions
        {
            get { return conventions ?? (conventions = new FilesConvention()); }
            set { conventions = value; }
        }

        /// <summary>
        /// Max number of cached requests (default: 2048)
        /// </summary>
        public int MaxNumberOfCachedRequests
        {
            get { return maxNumberOfCachedRequests; }
            set
            {
                maxNumberOfCachedRequests = value;
                if (jsonRequestFactory != null)
                    jsonRequestFactory.Dispose();
                jsonRequestFactory = new HttpJsonRequestFactory(maxNumberOfCachedRequests, HttpMessageHandler);
            }
        }

        public HttpMessageHandler HttpMessageHandler { get; set; }

        private string url;

        /// <summary>
        /// Gets or sets the URL.
        /// </summary>
        public virtual string Url
        {
            get { return url; }
            set { url = value.TrimEnd('/'); }
        }
        

        /// <summary>
        /// Gets or sets the identifier for this store.
        /// </summary>
        /// <value>The identifier.</value>
        public virtual string Identifier
        {
            get
            {
                if (identifier != null)
                    return identifier;
                if (Url == null)
                    return null;
                return Url;
            }
            set { identifier = value; }
        }
        private string identifier;


        public IFilesStore Initialize()
        {
            return Initialize(false);
        }

        public IFilesStore Initialize(bool ensureFileSystemExists)
        {
            if (initialized)
                return this;

            jsonRequestFactory = new HttpJsonRequestFactory(MaxNumberOfCachedRequests, HttpMessageHandler);

            try
            {
                InitializeInternal();

                initialized = true;

                if (ensureFileSystemExists && string.IsNullOrEmpty(DefaultFileSystem) == false)
                {
                    AsyncFilesCommands.ForFileSystem(DefaultFileSystem)
                                      .EnsureFileSystemExistsAsync()
                                      .Wait();
                }
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }

            return this;
        }

        protected virtual void InitializeInternal()
        {
            asyncFilesCommandsGenerator = () =>
            {
                return new AsyncFilesServerClient(Url, DefaultFileSystem, Conventions, new OperationCredentials(ApiKey, Credentials), jsonRequestFactory, currentSessionId);
            };
        }

        /// <summary>
        /// Generate new instance of files commands
        /// </summary>
        protected Func<IAsyncFilesCommands> asyncFilesCommandsGenerator;
        
        /// <summary>
        /// Gets the async file system commands.
        /// </summary>
        /// <value>The async file system commands.</value>
        public virtual IAsyncFilesCommands AsyncFilesCommands
        {
            get
            {
                AssertInitialized();
                var commands = asyncFilesCommandsGenerator();
                foreach (string key in SharedOperationsHeaders)
                {
                    var values = SharedOperationsHeaders.GetValues(key);
                    if (values == null)
                        continue;
                    foreach (var value in values)
                    {
                        commands.OperationsHeaders[key] = value;
                    }
                }
                return commands;
            }
        }

        public IAsyncFilesSession OpenAsyncSession()
        {
            throw new NotImplementedException();
        }

        public IAsyncFilesSession OpenAsyncSession(string filesystem)
        {
            throw new NotImplementedException();
        }

        public virtual IFilesSession OpenSession()
        {
            return OpenSession(new OpenFilesSessionOptions());
        }

        public virtual IFilesSession OpenSession(string filesystem)
        {
            return OpenSession(new OpenFilesSessionOptions
            {
                FileSystem = filesystem
            });
        }

        public virtual IFilesSession OpenSession(OpenFilesSessionOptions sessionOptions)
        {
            EnsureNotClosed();

            var sessionId = Guid.NewGuid();
            currentSessionId = sessionId;
            try
            {

                // TODO: Implement FileSession creation. 
                throw new NotImplementedException();
            }
            finally
            {
                currentSessionId = null;
            }
        }

        public FilesSessionListeners Listeners
        {
            get 
            {
                throw new NotImplementedException();        
                //return listeners; 
            }
        }
        public void SetListeners(FilesSessionListeners newListeners)
        {
            this.listeners = newListeners;

            throw new NotImplementedException();            
        }


        protected void EnsureNotClosed()
        {
            if (WasDisposed)
                throw new ObjectDisposedException(GetType().Name, "The files store has already been disposed and cannot be used");
        }

        protected void AssertInitialized()
        {
            if (!initialized)
                throw new InvalidOperationException("You cannot open a session or access the files commands before initializing the files store. Did you forget calling Initialize()?");
        }


        public event EventHandler AfterDispose = (obj, sender) => { };

        public bool WasDisposed
        {
            get;
            private set;
        }

        public void Dispose()
        {
#if DEBUG
            GC.SuppressFinalize(this);
#endif

            var tasks = new List<Task>();
            foreach (var fileSystemChange in fileSystemChanges)
            {
                var remoteFileSystemChanges = fileSystemChange.Value as FilesChangesClient;
                if (remoteFileSystemChanges != null)
                {
                    tasks.Add(remoteFileSystemChanges.DisposeAsync());
                }
                else
                {
                    using (fileSystemChange.Value as IDisposable) { }
                }
            }

            foreach (var fileSystemCommand in fileSystemCommands)
            {
                var remoteFileSystemCommand = fileSystemCommand.Value as IDisposable;
                if (remoteFileSystemCommand != null)
                    remoteFileSystemCommand.Dispose();
            }

            // try to wait until all the async disposables are completed
            Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(5));

            // if this is still going, we continue with disposal, it is for grace only, anyway
            if (jsonRequestFactory != null)
                jsonRequestFactory.Dispose();

            WasDisposed = true;
            AfterDispose(this, EventArgs.Empty);
        }
    }
}
