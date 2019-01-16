package com.paytm.daas.datastream.playground

import java.io.IOException
import java.util
import java.util.{HashMap => JHashMap, List => JList, Map => JMap}

import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
import org.apache.avro.Schema
import org.apache.flink.runtime.rest.util.RestClientException

/**
  * This class is needed because of IdentityHashMap problem in
  * io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient.
  * Check https://github.com/confluentinc/schema-registry/issues/619#issuecomment-345248325
  */
class CustomCachedSchemaRegistryClient(baseUrl: String, mapCapacity: Integer) extends SchemaRegistryClient {

  private val identityMapCapacity: Integer = mapCapacity
  private val restService: RestService = new RestService(baseUrl)
  private val idCache: JMap[String, JMap[Integer, Schema]] = new JHashMap[String, JMap[Integer, Schema]]()
  private val schemaCache: JMap[String, JMap[Schema, Integer]] = new JHashMap[String, JMap[Schema, Integer]]()
  private val versionCache: JMap[String, JMap[Schema, Integer]] = new JHashMap[String, JMap[Schema, Integer]]()
  val DEFAULT_REQUEST_PROPERTIES = new JHashMap[String, String]

  idCache.put(null, new JHashMap[Integer, Schema])
  DEFAULT_REQUEST_PROPERTIES.put("Content-Type", "application/vnd.schemaregistry.v1+json")

  @throws[IllegalStateException]
  override def register(subject: String, schema: Schema): Int = synchronized {
    val schemaIdMap: JMap[Schema, Integer] =
      if (schemaCache.containsKey(subject))
        schemaCache.get(subject)
      else {
        schemaCache.put(subject, new JHashMap[Schema, Integer])
        schemaCache.get(subject)
      }

    if (schemaIdMap.containsKey(schema)) {
      schemaIdMap.get(schema)
    } else if (schemaIdMap.size >= identityMapCapacity) {
      throw new IllegalStateException(s"Too many schema objects created for $subject!")
    } else {
      val id = registerAndGetId(subject, schema)
      schemaIdMap.put(schema, id)
      idCache.get(null).put(id, schema)
      id
    }
  }

  @throws[IOException]
  @throws[RestClientException]
  private def getSchemaByIdFromRegistry(id: Int) = {
    val restSchema = restService.getId(id)
    (new Schema.Parser).parse(restSchema.getSchemaString)
  }

  @throws[IOException]
  @throws[RestClientException]
  private def getVersionFromRegistry(subject: String, schema: Schema) =
    restService.lookUpSubjectVersion(schema.toString, subject, true).getVersion

  @throws[IOException]
  @throws[RestClientException]
  private def registerAndGetId(subject: String, schema: Schema) =
    restService.registerSchema(schema.toString, subject)

  @throws[IOException]
  @throws[RestClientException]
  private def getIdFromRegistry(subject: String, schema: Schema) =
    restService.lookUpSubjectVersion(schema.toString, subject, false).getId

  override def getByID(id: Int): Schema = getById(id)

  override def getById(id: Int): Schema = synchronized {
    getBySubjectAndId(null, id)
  }

  override def getBySubjectAndID(subject: String, id: Int): Schema = getBySubjectAndId(subject, id)

  override def getBySubjectAndId(subject: String, id: Int): Schema = synchronized {
    val idSchemaMap: JMap[Integer, Schema] =
      if (idCache.containsKey(subject))
        idCache.get(subject)
      else {
        idCache.put(subject, new JHashMap[Integer, Schema])
        idCache.get(subject)
      }

    if (idSchemaMap.containsKey(id))
      idSchemaMap.get(id)
    else {
      val schema = getSchemaByIdFromRegistry(id)
      idSchemaMap.put(id, schema)
      schema
    }
  }

  override def getLatestSchemaMetadata(subject: String): SchemaMetadata =
    synchronized {
      val response = restService.getLatestVersion(subject)
      val id = response.getId
      val version = response.getVersion
      val schema = response.getSchema
      new SchemaMetadata(id, version, schema)
    }

  override def getSchemaMetadata(subject: String,
                                 version: Int): SchemaMetadata = {
    val response = restService.getVersion(subject, version)
    val id = response.getId
    val schema = response.getSchema
    new SchemaMetadata(id, version, schema)
  }

  @throws[IllegalStateException]
  override def getVersion(subject: String, schema: Schema): Int = synchronized {
    val schemaVersionMap: JMap[Schema, Integer] =
      if (versionCache.containsKey(subject))
        versionCache.get(subject)
      else {
        versionCache.put(subject, new JHashMap[Schema, Integer])
        versionCache.get(subject)
      }

    if (schemaVersionMap.containsKey(schema))
      schemaVersionMap.get(schema)
    else if (schemaVersionMap.size >= identityMapCapacity)
      throw new IllegalStateException(s"Too many schema objects created for $subject!")
    else {
      val version = getVersionFromRegistry(subject, schema)
      schemaVersionMap.put(schema, version)
      version
    }
  }

  override def getAllVersions(subject: String): JList[Integer] = restService.getAllVersions(subject)

  override def getAllSubjects: util.Collection[String] = restService.getAllSubjects

  override def testCompatibility(subject: String, schema: Schema): Boolean =
    restService.testCompatibility(schema.toString, subject, "latest")

  override def updateCompatibility(subject: String, compatibility: String): String =
    restService.updateCompatibility(compatibility, subject).getCompatibilityLevel

  override def getCompatibility(subject: String): String =
    restService.getConfig(subject).getCompatibilityLevel


  @throws[IllegalStateException]
  override def getId(subject: String, schema: Schema): Int = synchronized {
    val schemaIdMap: JMap[Schema, Integer] =
      if (schemaCache.containsKey(subject))
        schemaCache.get(subject)
      else {
        schemaCache.put(subject, new JHashMap[Schema, Integer])
        schemaCache.get(subject)
      }

    if (schemaIdMap.containsKey(schema))
      schemaIdMap.get(schema)
    else if (schemaIdMap.size >= identityMapCapacity)
      throw new IllegalStateException("Too many schema objects created for " + subject + "!")
    else {
      val id = getIdFromRegistry(subject, schema)
      schemaIdMap.put(schema, id)
      idCache.get(null).put(id, schema)
      id
    }
  }

  override def deleteSubject(subject: String): JList[Integer] =
    deleteSubject(DEFAULT_REQUEST_PROPERTIES, subject)

  override def deleteSubject(requestProperties: JMap[String, String], subject: String): JList[Integer] = {
    versionCache.remove(subject)
    idCache.remove(subject)
    schemaCache.remove(subject)
    restService.deleteSubject(requestProperties, subject)
  }

  override def deleteSchemaVersion(subject: String, version: String): Integer =
    deleteSchemaVersion(DEFAULT_REQUEST_PROPERTIES, subject, version)

  override def deleteSchemaVersion(requestProperties: JMap[String, String],
                                   subject: String,
                                   version: String): Integer = {
    versionCache.get(subject).values.remove(Integer.valueOf(version))
    restService.deleteSchemaVersion(requestProperties, subject, version)
  }
}
