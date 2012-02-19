﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web;
using RavenFS.Util;
using Rdc.Wrapper;

namespace RavenFS.Rdc
{
    public class NeedListParser
    {
        public void Parse(IPartialDataAccess source, IPartialDataAccess seed, Stream output, IEnumerable<RdcNeed> needList)
        {
            // Currently it copies whole file but it should only replace changed pages
            // TODO: This cast from ulong to long can be dangerous     
            {
                foreach (var item in needList)
                {
                    switch (item.blockType)
                    {
                        case RdcNeedType.Source:
                            source.CopyTo(output, Convert.ToInt64(item.fileOffset), Convert.ToInt64(item.blockLength));
                            break;
                        case RdcNeedType.Seed:
                            seed.CopyTo(output, Convert.ToInt64(item.fileOffset), Convert.ToInt64(item.blockLength));
                            break;
                        default:
                            break;
                    }
                }
            }
        }
    }
}