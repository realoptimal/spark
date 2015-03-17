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

import java.util.List

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import org.apache.mesos.Protos._
import org.apache.mesos.{Scheduler, MesosSchedulerDriver}
import org.apache.spark.{Logging, SparkConf}
import org.apache.mesos.protobuf.ByteString
import org.apache.mesos.Protos.Value.Type


private[spark] trait MesosSchedulerUtils extends Logging {
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

  // Helper function to pull out a resource from a Mesos Resources protobuf
  def getResource(res: List[Resource], name: String): Double = {
    var resource = 0.0
    // A resource can have multiple values in the offer since it can either be from
    // a specific role or wildcard.
    for (r <- res if r.getName == name) {
      resource += r.getScalar.getValue

    }
    resource
  }

  /**
   * Partition the existing resource list based on the resources requested and
   * the remaining resources.
   * @return The remaining resources list and the used resources list.
   */
  def partitionResources(
      resources: List[Resource],
      resourceName: String,
      count: Double): (List[Resource], List[Resource]) = {
    var remain = count
    var usedResources = new ArrayBuffer[Resource]
    val newResources = resources.collect {
      case r => {
        if (remain > 0 &&
          r.getType == Type.SCALAR &&
          r.getScalar.getValue > 0.0 &&
          r.getName == resourceName) {
          val usage = Math.min(remain, r.getScalar.getValue)
          usedResources += Resource.newBuilder()
            .setName(resourceName)
            .setRole(r.getRole)
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder().setValue(usage).build())
            .build()
          remain -= usage
          Resource.newBuilder()
            .setName(resourceName)
            .setRole(r.getRole)
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder().setValue(r.getScalar.getValue - usage).build())
            .build()
        } else {
          r
        }
      }
    }

    (newResources.filter(r => r.getType != Type.SCALAR || r.getScalar.getValue > 0.0).toList,
      usedResources.toList)
  }
}
