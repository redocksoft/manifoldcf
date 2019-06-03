/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.manifoldcf.crawler.connectors.office365;

import org.apache.manifoldcf.agents.interfaces.ServiceInterruption;
import org.apache.manifoldcf.core.interfaces.ManifoldCFException;
import org.apache.manifoldcf.crawler.connectors.office365.functionalmanifold.ManifoldCheckedConsumer;
import org.apache.manifoldcf.crawler.connectors.office365.functionalmanifold.ThreadedObjectSupplier;
import org.apache.manifoldcf.crawler.connectors.office365.functionalmanifold.XThreadObjectBuffer;

public class Office365ThreadedObjectSupplier<T> extends ThreadedObjectSupplier<T> {
  public Office365ThreadedObjectSupplier(ManifoldCheckedConsumer<XThreadObjectBuffer<T>> block) {
    super(block);
  }

  @Override
  protected void handleOtherException(Exception e) throws ManifoldCFException, ServiceInterruption {
    Office365ErrorHandling.handleOtherException(e);
    super.handleOtherException(e);
  }
}
