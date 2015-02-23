/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster.mesos

import org.apache.mesos.Protos.{Credential, FrameworkInfo}
import org.apache.mesos.{Scheduler, MesosSchedulerDriver}
import org.apache.spark.SparkConf
import org.apache.mesos.protobuf.ByteString

private[spark] trait MesosSchedulerUtils {
  def createSchedulerDriver(
      scheduler: Scheduler,
      sparkUser: String,
      appName: String,
      masterUrl: String,
      conf: SparkConf) = {
    val fwInfoBuilder = FrameworkInfo.newBuilder().setUser(sparkUser).setName(appName)
    var credBuilder: Option[Credential.Builder] = None

    conf.getOption("spark.mesos.principal").foreach { principal =>
      fwInfoBuilder.setPrincipal(principal)
      credBuilder = Option(Credential.newBuilder().setPrincipal(principal))
    }

    conf.getOption("spark.mesos.secret").foreach { secret =>
      credBuilder = Option(credBuilder.getOrElse(Credential.newBuilder())
        .setSecret(ByteString.copyFromUtf8(secret)))
    }

    conf.getOption("spark.mesos.role").foreach { role =>
      fwInfoBuilder.setRole(role)
    }

    if (credBuilder.isDefined) {
      new MesosSchedulerDriver(
        scheduler, fwInfoBuilder.build, masterUrl, credBuilder.get.build())
    } else {
      new MesosSchedulerDriver(scheduler, fwInfoBuilder.build, masterUrl)
    }
  }
}
