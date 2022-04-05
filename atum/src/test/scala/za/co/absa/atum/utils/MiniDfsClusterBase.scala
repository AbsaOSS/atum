///*
// * Copyright 2018 ABSA Group Limited
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package za.co.absa.atum.utils
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.StorageType
//import org.apache.hadoop.hdfs.MiniDFSCluster
//import org.scalatest.{BeforeAndAfterAll, Suite}
//
//trait MiniDfsClusterBase extends BeforeAndAfterAll { this: Suite =>
//
//  protected def getConfiguration: Configuration = new Configuration()
//
//  private val miniDFSCluster = new MiniDFSCluster.Builder(getConfiguration)
//    .numDataNodes(2)
//    .storageTypes(Array(StorageType.RAM_DISK))
//    .build()
//  implicit val fs = miniDFSCluster.getFileSystem()
//
//  override def afterAll(): Unit = {
//    miniDFSCluster.shutdown()
//  }
//}
