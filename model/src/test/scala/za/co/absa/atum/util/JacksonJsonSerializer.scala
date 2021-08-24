/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.atum.util

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.reflect.ClassTag
import scala.util.Try

/**
 * Sample serializer that is expected to be used for Atum's model externally, e.g. in Enceladus
 */
object JacksonJsonSerializer {

  val objectMapper: ObjectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .setSerializationInclusion(Include.NON_EMPTY) // e.g. null-values fields omitted


  def fromJson[T](json: String)
                 (implicit ct: ClassTag[T]): T = {
    val clazz = ct.runtimeClass.asInstanceOf[Class[T]]
    if (clazz == classOf[String]) {
      json.asInstanceOf[T]
    } else {
      objectMapper.readValue(json, clazz)
    }
  }

  def toJson[T](entity: T): String = {
    entity match {
      case str: String =>
        if (isValidJson(str)) str else objectMapper.writeValueAsString(entity)
      case _ =>
        objectMapper.writeValueAsString(entity)
    }
  }

  def isValidJson[T](str: T with String): Boolean = {
    Try(objectMapper.readTree(str)).isSuccess
  }

}
