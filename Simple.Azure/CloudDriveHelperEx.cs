using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Simple.Azure
{
    public static class CloudDriveHelperEx
    {
        public static bool TryUnmount(this CloudDriveHelper cloudDriveHelper)
        {
            if (cloudDriveHelper != null)
            {
                try
                {
                    cloudDriveHelper.Unmount();
                    return true;
                }
                catch (Exception ex)
                {
                    Trace.WriteLine(ex.ToString());
                }
            }

            return false;
        }
    }
}
