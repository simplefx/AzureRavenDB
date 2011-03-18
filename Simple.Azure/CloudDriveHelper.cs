using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.StorageClient;

namespace Simple.Azure
{
    public class CloudDriveHelper
    {
        private readonly string _containerName;
        private readonly string _vhdName;

        private CloudDrive _ravenDataDrive;
        LocalResource _localCache;
        private CloudBlobContainer _ravenDrives;
        readonly CloudStorageAccount _ravenDataStorageAccount = CloudStorageAccount.Parse(RoleEnvironment.GetConfigurationSettingValue("StorageAccount"));
        private CloudBlobClient _blobClient;

        public CloudDriveHelper(string containerName, string vhdName)
        {
            _containerName = containerName;
            _vhdName = vhdName;
        }

        public string MountCloudDrive()
        {
            _blobClient = _ravenDataStorageAccount.CreateCloudBlobClient();

            InitializeLocalCache();

            CreateDrivesContainer();

            SetContainerPermissions();

            CreateDrive();

            var ravenDrivePath = _ravenDataDrive.Mount(_localCache.MaximumSizeInMegabytes, DriveMountOptions.Force);

            return ravenDrivePath;
        }

        private void InitializeLocalCache()
        {
            _localCache = RoleEnvironment.GetLocalResource("RavenCache");

            CloudDrive.InitializeCache(_localCache.RootPath.TrimEnd('\\'), _localCache.MaximumSizeInMegabytes);
        }

        private void SetContainerPermissions()
        {
            BlobContainerPermissions permissions = _ravenDrives.GetPermissions();
            permissions.PublicAccess = BlobContainerPublicAccessType.Container;
            _ravenDrives.SetPermissions(permissions);
        }

        private void CreateDrivesContainer()
        {
            _ravenDrives = _blobClient.GetContainerReference(_containerName);
            _ravenDrives.CreateIfNotExist();
        }

        private void CreateDrive()
        {
            try
            {
                var vhdUrl = _blobClient.GetContainerReference(_containerName).GetPageBlobReference(_vhdName).Uri.ToString();
                _ravenDataDrive = _ravenDataStorageAccount.CreateCloudDrive(vhdUrl);
                _ravenDataDrive.Create(_localCache.MaximumSizeInMegabytes);
            }
            catch (CloudDriveException ex)
            {
                Trace.TraceWarning(ex.Message);
            }
        }
    }

}
