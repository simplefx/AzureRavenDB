using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using Microsoft.WindowsAzure.ServiceRuntime;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Http;
using Simple.Azure;

namespace RavenWorker
{
    public class WorkerRole : RoleEntryPoint
    {
        private DocumentDatabase _database;
        private RavenDbHttpServer _server;
        private CloudDriveHelper _cloudDriveHelper;

        public override void Run()
        {
            Trace.WriteLine("RavenWorker entry point called", "Information");

            while (true)
            {
                Thread.Sleep(10000);
                Trace.WriteLine("Working", "Information");
            }
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections 
            ServicePointManager.DefaultConnectionLimit = 12;

            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            var drive = MountCloudDrive();

            var config = new RavenConfiguration
                             {
                                 DataDirectory = drive,
                                 AnonymousUserAccessMode = AnonymousUserAccessMode.All,
                                 HttpCompression = true,
                                 DefaultStorageTypeName = "munin",
                                 Port = MyInstanceEndpoint.IPEndpoint.Port,
                                 PluginsDirectory = "plugins"
                             };
            
            StartRaven(config);
            SetupReplication();

            RoleEnvironment.Changed += RoleEnvironmentChanged;
            RoleEnvironment.StatusCheck += RoleEnvironmentStatusCheck;

            return base.OnStart();
        }

        static void RoleEnvironmentStatusCheck(object sender, RoleInstanceStatusCheckEventArgs e)
        {
            Trace.WriteLine(e.Status);
        }

        void RoleEnvironmentChanged(object sender, RoleEnvironmentChangedEventArgs e)
        {
            if (e.Changes.OfType<RoleEnvironmentTopologyChange>().Any())
            {
                SetupReplication();
            }
        }

        protected RoleInstanceEndpoint MyInstanceEndpoint
        {
            get { return RoleEnvironment.CurrentRoleInstance.InstanceEndpoints["Raven"]; }
        }

        private void SetupReplication()
        {
            if (RoleEnvironment.CurrentRoleInstance.Role.Instances.Count < 2)
            {
                var tr = new TransactionInformation();
                _database.Put("Debug", null,
                              JObject.Parse(@"{""Url"":""" +
                                            GetEndPointAddress(MyInstanceEndpoint.IPEndpoint) + @"""}"),
                              new JObject(), tr);

                _database.Commit(tr.Id);

            }
            else
            {
                var json = BuildDestinationsString();
                Trace.WriteLine(json);

                var tr = new TransactionInformation();
                _database.Delete("Raven/Replication/Destinations", null, tr);
                _database.Put("Raven/Replication/Destinations", null, JObject.Parse(json),
                              new JObject(), tr);
                _database.Commit(tr.Id);
            }
        }

        private static string BuildDestinationsString()
        {
            var json = new StringBuilder(@"{""Destinations"":[");
            foreach (var roleInstance in RoleEnvironment.CurrentRoleInstance.Role.Instances.Where(instance => instance.Id != RoleEnvironment.CurrentRoleInstance.Id))
            {
                RoleInstanceEndpoint endpoint;
                if (roleInstance.InstanceEndpoints.TryGetValue("Replication", out endpoint))
                {
                    json.AppendFormat(@"{{""Url"":""{0}""}}",
                                      GetEndPointAddress(endpoint.IPEndpoint));
                }
                else
                {
                    foreach (var instanceEndpoint in roleInstance.InstanceEndpoints)
                    {
                        Trace.WriteLine(string.Format("Instance endpoint: {0}: {1}", instanceEndpoint.Key, instanceEndpoint.Value.IPEndpoint.Address));
                    }
                }
            }
            json.Append("]}");
            return json.ToString();
        }

        private static string GetEndPointAddress(IPEndPoint endPoint)
        {
            return string.Format("http://{0}:{1}/", endPoint.Address, RoleEnvironment.CurrentRoleInstance.InstanceEndpoints["Raven"].IPEndpoint.Port);
        }

        private string MountCloudDrive()
        {
            _cloudDriveHelper = new CloudDriveHelper("raven", RoleEnvironment.CurrentRoleInstance.Id + ".vhd");
            var drive = _cloudDriveHelper.MountCloudDrive();

            if (!drive.EndsWith("\\")) drive += "\\";
            return drive;
        }

        public override void OnStop()
        {
            StopRaven();
            _cloudDriveHelper.TryUnmount();
            base.OnStop();
        }

        private void StartRaven(RavenConfiguration config)
        {
            try
            {
                _database = new DocumentDatabase(config);
                _database.SpinBackgroundWorkers();
                _server = new RavenDbHttpServer(config, _database);
                try
                {
                    _server.Start();
                }
                catch (Exception)
                {
                    _server.Dispose();
                    _server = null;
                    throw;
                }
            }
            catch (Exception)
            {
                _database.Dispose();
                _database = null;
                throw;
            }
        }

        private void StopRaven()
        {
            if (_server != null)
            {
                _server.Dispose();
                _server = null;
            }
            if (_database != null)
            {
                _database.Dispose();
                _database = null;
            }
        }
    }
}
