/* $Id: WaitJobPaused.java 988245 2010-08-23 18:39:35Z kwright $ */

/**
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
* 
* http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.manifoldcf.crawler;

import java.io.*;
import org.apache.manifoldcf.core.interfaces.*;
import org.apache.manifoldcf.crawler.interfaces.*;
import org.apache.manifoldcf.crawler.system.*;
import java.util.*;

/** This class is used during testing.
*/
public class WaitJobPaused
{
  public static final String _rcsid = "@(#)$Id: WaitJobPaused.java 988245 2010-08-23 18:39:35Z kwright $";

  private WaitJobPaused()
  {
  }

  // Add: throttle, priority, recrawl interval

  public static void main(String[] args)
  {
    if (args.length != 1)
    {
      System.err.println("Usage: WaitJobPaused <jobid>");
      System.exit(1);
    }

    String jobID = args[0];


    try
    {
      IThreadContext tc = ThreadContextFactory.make();
      ManifoldCF.initializeEnvironment(tc);
      IJobManager jobManager = JobManagerFactory.make(tc);
      while (true)
      {
        if (jobManager.checkJobBusy(new Long(jobID)))
        {
          ManifoldCF.sleep(5000);
          continue;
        }
        break;
      }
      System.err.println("Job no longer busy");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      System.exit(2);
    }
  }
    
}
